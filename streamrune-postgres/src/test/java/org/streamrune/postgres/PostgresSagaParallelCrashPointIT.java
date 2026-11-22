package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.postgres.CrashItSupport.dataSource;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.postgres.SagaParallelPostgresCrashPoints.NodeCrashed;
import org.streamrune.postgres.SagaParallelPostgresCrashPoints.Point;
import org.streamrune.postgres.SagaParallelPostgresNodes.EndState;
import org.streamrune.postgres.SagaParallelPostgresNodes.Node;
import org.streamrune.postgres.SagaParallelPostgresNodes.ParState;
import org.streamrune.postgres.SagaParallelPostgresNodes.Replied;
import org.streamrune.postgres.SagaParallelPostgresNodes.Row;
import org.streamrune.postgres.SagaParallelPostgresNodes.Started;
import org.streamrune.postgres.SagaParallelPostgresNodes.Stepped;
import org.streamrune.postgres.SagaParallelPostgresNodes.Undone;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The crash points of a page stepped in parallel that cut between transactions or between
 * processes, on PostgreSQL: a rerun after the crash ends where an uninterrupted run ends. The
 * fence's SQL invariant ({@link SagaParallelPostgresNodes#lateForwardCommands()}: no forward
 * command stored after its saga's first compensation command) is checked with a pair to check only
 * by {@link #aForwardCommandHeldPastAnotherProcessesClaim_isRefusedAtItsCommit()}, the one case
 * with a saga that has both a stored forward key and a compensation key; elsewhere it is a guard.
 *
 * <p>Each crash case runs an uninterrupted reference over a clean database, then cleans it — the
 * global offsets start at 1 again, so the same events land at the same offsets and the commands of
 * a step carry the same keys — and runs the interrupted schedule. The page holds three sagas, two
 * in flight at once: the step of {@code a} is held in its forward command until {@code b} reaches
 * the crash point (or wrote its row, where the page has to end first), and {@code c} is not started
 * before the crash. A crashed node is abandoned and a fresh one runs until nothing is left. The
 * offsets a run records are compared by the event they name ({@link
 * SagaParallelPostgresNodes.EndState}).
 */
@Testcontainers
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class PostgresSagaParallelCrashPointIT {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("saga_parallel_crash");

  private static final String SAGA_A = "a";
  private static final String SAGA_B = "b";
  private static final String SAGA_C = "c";
  private static final Duration AWAIT = Duration.ofSeconds(30);
  private static final int MAX_CYCLES = 50;

  static PGSimpleDataSource source;

  private SagaParallelPostgresNodes nodes;

  @BeforeAll
  static void schema() {
    source = dataSource(PG, "par-crash");
    new PostgresEventStoreFactory(source, SagaParallelPostgresNodes.REGISTRY).create();
  }

  @BeforeEach
  void fresh() throws Exception {
    nodes = new SagaParallelPostgresNodes(source);
    nodes.clean();
  }

  @AfterEach
  void closeAll() {
    nodes.closeAll();
  }

  @ParameterizedTest
  @EnumSource(value = Point.class, mode = EnumSource.Mode.EXCLUDE, names = "AFTER_SAVE_OFFSET")
  void aRerunAfterACrashEndsWhereAnUninterruptedRunEnds(Point point) throws Exception {
    boolean compensates = isCompensation(point);
    EndState uninterrupted = uninterrupted(compensates);

    begin(compensates);
    runUntilCrash(point);
    assertStateLeft(point);
    assertSameEnd(runToQuiescence(nodes.node()), uninterrupted);
    assertNoLateForwardCommandGuard();
  }

  /**
   * Two processes over the database, no lease, poll the same page at once; the first forward
   * command of each saga waits until both are in it, so the two steps of every saga overlap. The
   * step of {@code b} is rejected, so both processes claim its compensation. Each step is applied
   * once and the compensation runs once: the end state is one process's.
   */
  @Test
  void twoProcessesDeliverOnePageInParallel_eachStepIsAppliedOnce() throws Exception {
    EndState oneNode = uninterrupted(true);

    begin(true);
    Node first = nodes.node();
    Node second = nodes.node();
    Map<String, CountDownLatch> bothIn = new ConcurrentHashMap<>();
    for (Node node : List.of(first, second)) {
      Set<String> entered = ConcurrentHashMap.newKeySet();
      node.crash.beforeCall =
          (method, sagaId) -> {
            if (method.equals("Step") && entered.add(sagaId)) {
              CountDownLatch latch = bothIn.computeIfAbsent(sagaId, id -> new CountDownLatch(2));
              latch.countDown();
              await(latch, "both processes in the step of " + sagaId);
            }
          };
    }
    Thread a = Thread.ofVirtual().start(() -> drainQuietly(first));
    Thread b = Thread.ofVirtual().start(() -> drainQuietly(second));
    assertThat(a.join(AWAIT)).isTrue();
    assertThat(b.join(AWAIT)).isTrue();
    assertThat(bothIn.keySet()).containsExactlyInAnyOrder(SAGA_A, SAGA_B, SAGA_C);
    assertThat(bothIn.values())
        .as("the two processes were in the step of every saga at once")
        .allMatch(latch -> latch.getCount() == 0);

    runToQuiescence(first);
    assertSameEnd(runToQuiescence(second), oneNode);
    assertThat(nodes.eventsOfSaga(SAGA_B).stream().filter(new Undone(SAGA_B)::equals)).hasSize(2);
    assertNoLateForwardCommandGuard();
  }

  /**
   * A process is paused between the end of its page and its checkpoint save, while another delivers
   * everything and saves the head; the paused save then lands late and the deposed process stops
   * right after it. The monotonic guard keeps the checkpoint at the head.
   */
  @Test
  void aLateCheckpointSaveOfADeposedProcessDoesNotMoveTheCheckpointBack() throws Exception {
    EndState oneNode = uninterrupted(false);

    begin(false);
    Node deposed = nodes.node(Point.AFTER_SAVE_OFFSET, null);
    CountDownLatch atTheSave = new CountDownLatch(1);
    CountDownLatch resume = new CountDownLatch(1);
    AtomicBoolean paused = new AtomicBoolean();
    deposed.crash.beforeCall =
        (method, sagaId) -> {
          if (method.equals("saveOffset") && paused.compareAndSet(false, true)) {
            atTheSave.countDown();
            await(resume, "the late save's release");
          }
        };
    AtomicReference<Throwable> deposedOutcome = new AtomicReference<>();
    Thread late =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    deposed.pollOnce();
                  } catch (Throwable t) {
                    deposedOutcome.set(t);
                  }
                });
    await(atTheSave, "the deposed process at its checkpoint save");
    assertThat(nodes.checkpoint()).as("its page ended, its save is pending").isZero();

    EndState byTheLeader = runToQuiescence(nodes.node());
    long head = nodes.checkpoint();
    assertThat(head).isEqualTo((long) nodes.stored().size()).isGreaterThan(3L);

    resume.countDown();
    assertThat(late.join(AWAIT)).isTrue();
    assertThat(deposedOutcome.get()).isInstanceOf(NodeCrashed.class);
    assertThat(nodes.checkpoint()).as("the late save of offset 3 is a no-op").isEqualTo(head);
    assertSameEnd(byTheLeader, oneNode);
    assertSameEnd(nodes.endState(), oneNode);
    assertNoLateForwardCommandGuard();
  }

  /**
   * A saga's first forward command has committed and its second is held before it reaches the event
   * store, when another process's timeout runner claims the saga and compensates it. The held
   * command is then refused at its commit, so the saga has a forward key and a compensation key and
   * no forward command is stored after the compensation: the fence's SQL invariant holds with a
   * pair to check.
   */
  @Test
  void aForwardCommandHeldPastAnotherProcessesClaim_isRefusedAtItsCommit() throws Exception {
    nodes.append(new Started(SAGA_B), new Replied(SAGA_B));
    Node stepping = nodes.node();
    Node claiming = nodes.node();
    CountDownLatch held = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger stepsOfB = new AtomicInteger();
    stepping.crash.beforeCall =
        (method, sagaId) -> {
          if (method.equals("Step") && SAGA_B.equals(sagaId) && stepsOfB.incrementAndGet() == 2) {
            held.countDown();
            await(release, "the second forward command's release");
          }
        };
    Thread delivering = Thread.ofVirtual().start(() -> drainQuietly(stepping, false));
    await(held, "b's second forward command");

    nodes.timeOut(claiming);
    assertThat(nodes.row(SAGA_B).orElseThrow().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(nodes.eventsOfSaga(SAGA_B)).contains(new Undone(SAGA_B));

    release.countDown();
    assertThat(delivering.join(AWAIT)).isTrue();
    drainQuietly(stepping, false);

    assertThat(nodes.eventsOfSaga(SAGA_B))
        .as("the held forward command was refused: nothing of it is stored")
        .containsExactly(
            new Started(SAGA_B), new Replied(SAGA_B), new Stepped(SAGA_B, 1), new Undone(SAGA_B));
    assertThat(nodes.endState().inboxKeys())
        .as("the saga has a stored forward key and a compensation key")
        .contains("saga:b:1:0")
        .anyMatch(key -> key.startsWith("saga:b:episode:"));
    assertThat(nodes.row(SAGA_B).orElseThrow().status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(nodes.checkpoint()).isEqualTo((long) nodes.stored().size());
    assertThat(nodes.lateForwardCommands()).isEmpty();
  }

  // ==================== the runs ====================

  private static boolean isCompensation(Point point) {
    return switch (point) {
      case AFTER_COMPENSATION_CLAIM, AFTER_FIRST_COMPENSATION_COMMAND, BEFORE_TERMINAL_WRITE ->
          true;
      default -> false;
    };
  }

  /**
   * A clean database with the page of {@code a}, {@code b} and {@code c} appended; a rejected
   * {@code b} is compensated by two commands.
   */
  private void begin(boolean bIsRejected) throws Exception {
    nodes.closeAll();
    nodes.clean();
    if (bIsRejected) {
      nodes.rejecting.add(SAGA_B);
      nodes.compensationCommands = 2;
    }
    nodes.append(new Started(SAGA_A), new Started(SAGA_B), new Started(SAGA_C));
  }

  /** The reference: one node over a clean database, run to its end. */
  private EndState uninterrupted(boolean bIsRejected) throws Exception {
    begin(bIsRejected);
    EndState end = runToQuiescence(nodes.node());
    assertThat(end.rowsBySaga().keySet()).containsExactlyInAnyOrder(SAGA_A, SAGA_B, SAGA_C);
    for (Map.Entry<String, Row> row : end.rowsBySaga().entrySet()) {
      SagaStatus expected =
          bIsRejected && row.getKey().equals(SAGA_B) ? SagaStatus.COMPENSATED : SagaStatus.RUNNING;
      assertThat(row.getValue().status()).as(row.getKey()).isEqualTo(expected);
      if (expected == SagaStatus.RUNNING) {
        assertThat(row.getValue().applied()).as(row.getKey()).hasSize(2);
      }
    }
    assertThat(end.deadLettersBySaga()).isEmpty();
    assertNoLateForwardCommandGuard();
    return end;
  }

  /**
   * Runs a node that stops at {@code point} on {@code b}: {@code a}'s forward command is held until
   * the crash (or until {@code b}'s row write, for the point after the page), and {@code b} starts
   * only once {@code a} is held.
   */
  private void runUntilCrash(Point point) {
    Node node = nodes.node(point, SAGA_B);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch aEntered = new CountDownLatch(1);
    AtomicBoolean held = new AtomicBoolean();
    node.crash.beforeCall =
        (method, sagaId) -> {
          if (method.equals("Step") && SAGA_A.equals(sagaId) && held.compareAndSet(false, true)) {
            aEntered.countDown();
            await(release, "a's release");
          }
          if (SAGA_B.equals(sagaId)) {
            await(aEntered, "a's forward command, before b starts");
          }
        };
    node.crash.onCrash = release::countDown;
    if (point == Point.BEFORE_SAVE_OFFSET) {
      node.crash.afterCall =
          (method, sagaId) -> {
            if (SAGA_B.equals(sagaId) && method.equals("applyEvent")) {
              release.countDown();
            }
          };
    }
    for (int cycle = 0; cycle < MAX_CYCLES && !node.crash.crashed(); cycle++) {
      try {
        node.cycle();
      } catch (NodeCrashed crashed) {
        break;
      } catch (RuntimeException failedPage) {
        // delivered again on the next cycle
      }
    }
    assertThat(node.crash.crashed()).as("the node reached " + point).isTrue();
  }

  /** What the crash left, so the case is known to have stopped where it says. */
  private void assertStateLeft(Point point) {
    assertThat(nodes.checkpoint()).as("the crashed page is not acknowledged").isZero();
    switch (point) {
      case AFTER_FORWARD_COMMAND_COMMIT -> {
        assertGenesisPending(SAGA_B);
        assertThat(nodes.eventsOfSaga(SAGA_B)).contains(new Stepped(SAGA_B, 2));
        assertMidStepAndNotStarted();
      }
      case AFTER_ROW_WRITE -> {
        assertThat(nodes.row(SAGA_B).orElseThrow().genesisApplied()).isTrue();
        assertMidStepAndNotStarted();
      }
      case AFTER_COMPENSATION_CLAIM -> {
        assertThat(nodes.row(SAGA_B).orElseThrow().status()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(nodes.eventsOfSaga(SAGA_B)).doesNotContain(new Undone(SAGA_B));
        assertMidStepAndNotStarted();
      }
      case AFTER_FIRST_COMPENSATION_COMMAND -> {
        assertThat(nodes.row(SAGA_B).orElseThrow().status()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(nodes.eventsOfSaga(SAGA_B))
            .as("between the two compensation commands")
            .containsOnlyOnce(new Undone(SAGA_B));
        assertMidStepAndNotStarted();
      }
      case BEFORE_TERMINAL_WRITE -> {
        assertThat(nodes.row(SAGA_B).orElseThrow().status()).isEqualTo(SagaStatus.COMPENSATING);
        assertThat(nodes.eventsOfSaga(SAGA_B).stream().filter(new Undone(SAGA_B)::equals))
            .as("both compensation commands committed")
            .hasSize(2);
        assertMidStepAndNotStarted();
      }
      case BEFORE_SAVE_OFFSET -> {
        for (String sagaId : List.of(SAGA_A, SAGA_B, SAGA_C)) {
          assertThat(nodes.row(sagaId).orElseThrow().genesisApplied()).as(sagaId).isTrue();
        }
      }
      case AFTER_SAVE_OFFSET -> throw new IllegalArgumentException("not a case of this test");
    }
  }

  private void assertGenesisPending(String sagaId) {
    LoadedSaga<ParState> row = nodes.row(sagaId).orElseThrow();
    assertThat(row.genesisApplied()).as(sagaId + " genesis applied").isFalse();
    assertThat(row.version()).as(sagaId + " version").isEqualTo(1L);
  }

  /** {@code a} was cut mid-step (its row written, its command not), {@code c} never started. */
  private void assertMidStepAndNotStarted() {
    assertGenesisPending(SAGA_A);
    assertThat(nodes.eventsOfSaga(SAGA_A)).containsExactly(new Started(SAGA_A));
    assertThat(nodes.row(SAGA_C)).isEmpty();
  }

  /** The two end states are equal; the stored events first, as every other part names them. */
  private static void assertSameEnd(EndState actual, EndState expected) {
    assertThat(actual.eventsByAggregate()).isEqualTo(expected.eventsByAggregate());
    assertThat(actual.rowsBySaga()).isEqualTo(expected.rowsBySaga());
    assertThat(actual.inboxKeys()).isEqualTo(expected.inboxKeys());
    assertThat(actual.deadLettersBySaga()).isEqualTo(expected.deadLettersBySaga());
    assertThat(actual.checkpoint()).isEqualTo(expected.checkpoint());
    assertThat(actual.commandsBySaga()).isEqualTo(expected.commandsBySaga());
  }

  /** Runs cycles of the node until one delivers nothing; returns the end state. */
  private EndState runToQuiescence(Node node) throws Exception {
    RuntimeException last = null;
    for (int cycle = 0; cycle < MAX_CYCLES; cycle++) {
      try {
        if (node.cycle() == 0) {
          assertThat(nodes.checkpoint())
              .as("every event delivered")
              .isEqualTo((long) nodes.stored().size());
          return nodes.endState();
        }
      } catch (RuntimeException failedPage) {
        last = failedPage;
      }
    }
    throw new AssertionError("no quiescence after " + MAX_CYCLES + " cycles", last);
  }

  private static void drainQuietly(Node node) {
    drainQuietly(node, true);
  }

  /** Runs cycles (or, without the timeout runner, polls) until one delivers nothing. */
  private static void drainQuietly(Node node, boolean withTimeouts) {
    for (int cycle = 0; cycle < MAX_CYCLES; cycle++) {
      try {
        if ((withTimeouts ? node.cycle() : node.pollOnce()) == 0) {
          return;
        }
      } catch (RuntimeException failedPage) {
        // delivered again on the next cycle
      }
    }
  }

  private static void await(CountDownLatch latch, String what) {
    try {
      if (!latch.await(AWAIT.toMillis(), TimeUnit.MILLISECONDS)) {
        throw new AssertionError("timed out waiting for " + what);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted waiting for " + what, e);
    }
  }

  /**
   * The fence's invariant as a guard: no saga of these cases has both a stored forward key and a
   * compensation key, so it has no pair to check (only the fence test gives it one).
   */
  private void assertNoLateForwardCommandGuard() throws Exception {
    assertThat(nodes.lateForwardCommands()).isEmpty();
  }
}
