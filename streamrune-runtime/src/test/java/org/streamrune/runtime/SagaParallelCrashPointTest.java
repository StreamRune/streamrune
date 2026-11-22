package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.CrashingStores.CHECKPOINT;
import static org.streamrune.runtime.SagaParallelFixtures.AWAIT;
import static org.streamrune.runtime.SagaParallelFixtures.NULL_SAGA;
import static org.streamrune.runtime.SagaParallelFixtures.SAGA_TYPE;
import static org.streamrune.runtime.SagaParallelFixtures.aggregateOf;
import static org.streamrune.runtime.SagaParallelFixtures.await;
import static org.streamrune.runtime.SagaParallelFixtures.idOf;
import static org.streamrune.runtime.SagaParallelFixtures.orchestrator;

import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.streamrune.core.Command;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.runtime.CrashingStores.Node;
import org.streamrune.runtime.CrashingStores.NodeCrashed;
import org.streamrune.runtime.CrashingStores.Point;
import org.streamrune.runtime.SagaParallelFixtures.Delegating;
import org.streamrune.runtime.SagaParallelFixtures.EndState;
import org.streamrune.runtime.SagaParallelFixtures.ParState;
import org.streamrune.runtime.SagaParallelFixtures.Replied;
import org.streamrune.runtime.SagaParallelFixtures.Started;
import org.streamrune.runtime.SagaParallelFixtures.Stepped;
import org.streamrune.runtime.SagaParallelFixtures.Undo;
import org.streamrune.runtime.SagaParallelFixtures.Undone;
import org.streamrune.runtime.SagaParallelFixtures.World;
import org.streamrune.test.MutableClock;

/**
 * A rerun after a crash at any point of a page that steps its sagas in parallel ends where an
 * uninterrupted run ends: one test per point at which a node can stop while it delivers a page, or
 * at which another node, a timeout claim, a replay feed or an interrupt meets the page.
 *
 * <p>A node is a runner, its subscription and its timeout runner over the shared stores of a {@link
 * World} ({@link CrashingStores}); it steps the sagas of a page two at a time. In the crash-point
 * cases the page holds three sagas (five where a third failure has to stop it): the step of {@code
 * a} is held in its forward command, so it is mid-step when {@code b} reaches the crash point, and
 * {@code c} is not started (an {@link Error} stops the page) — or, where the case is about a failed
 * page that runs on, runs after {@code b}. Where the page has to end, or to fail, before the crash,
 * {@code a} is released once {@code b}'s row write is made or once the worker of the first failure
 * has ended, so what the page started is decided before {@code a} goes on. The crashed node is
 * abandoned; a fresh node with no crash point runs over the same stores until nothing is left to
 * deliver, and its end state is compared with that of a run of the same page that neither crashed
 * nor failed.
 *
 * <p>The replies to the sagas' commands are stored while the pages run, so their offsets depend on
 * the order in which the sagas' commands committed, which a crash and a rerun change. The offsets a
 * run records — each saga's applied offsets and last applied offset, each dead-letter entry's
 * offset — are therefore compared by the event each one names ({@code stream@version}), not by
 * number: exactly, and independent of that order ({@link #canonical}). Everything else is compared
 * as it is stored; the commands dispatched for each saga are compared with repeats removed, as a
 * rerun dispatches a committed command again and the inbox answers it.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class SagaParallelCrashPointTest {

  private static final String SAGA_A = "a";
  private static final String SAGA_B = "b";
  private static final String SAGA_C = "c";
  private static final String SAGA_D = "d";
  private static final String SAGA_E = "e";

  /** A {@link Replied} that no saga can be routed for: its routing throws. */
  private static final String UNROUTABLE = "unroutable";

  /** The parallelism of every node: {@code a} and {@code b} in flight, {@code c} waiting. */
  private static final int PARALLELISM = 2;

  /** The bound of the poll cycles of one run. */
  private static final int MAX_CYCLES = 50;

  // ==================== the crash points of a page ====================

  @ParameterizedTest
  @EnumSource(Point.class)
  void aRerunAfterACrashEndsWhereAnUninterruptedRunEnds(Point point) {
    Scenario scenario = Scenario.of(point);
    Canon uninterrupted = uninterrupted(scenario);

    try (Run crashed = new Run(scenario)) {
      crashed.runUntilCrash(point);
      scenario.assertStateLeft(point, crashed);
      Canon rerun = crashed.runToQuiescence(crashed.stores.node());
      assertSameEnd(rerun, uninterrupted);
    }
  }

  // ==================== between processes, threads and other writers ====================

  /**
   * Two nodes over the same stores, no lease, poll the same page at once; the first forward command
   * of each saga waits until both nodes are in it, so the two steps of every saga overlap. The step
   * of {@code b} is rejected, so both nodes claim its compensation. Each step is applied once and
   * the compensation runs once: the end state is one node's.
   */
  @Test
  void twoNodesDeliverOnePageInParallel_eachStepIsAppliedOnce() throws Exception {
    Scenario scenario =
        Scenario.builder().rejecting(SAGA_B).ending(SAGA_B, SagaStatus.COMPENSATED).build();
    Canon oneNode = uninterrupted(scenario);
    try (Run run = new Run(scenario)) {
      Node first = run.stores.node();
      Node second = run.stores.nodeOfAnotherProcess();
      Map<String, CountDownLatch> bothIn = new ConcurrentHashMap<>();
      for (Node node : List.of(first, second)) {
        Set<String> entered = ConcurrentHashMap.newKeySet();
        node.beforeCall =
            (method, sagaId) -> {
              if (method.equals("Step") && entered.add(sagaId)) {
                CountDownLatch latch = bothIn.computeIfAbsent(sagaId, id -> new CountDownLatch(2));
                latch.countDown();
                await(latch, "both nodes in the step of " + sagaId);
              }
            };
      }

      Thread a = Thread.ofVirtual().start(() -> run.drainQuietly(first));
      Thread b = Thread.ofVirtual().start(() -> run.drainQuietly(second));
      assertThat(a.join(AWAIT.multipliedBy(4))).isTrue();
      assertThat(b.join(AWAIT.multipliedBy(4))).isTrue();
      assertThat(bothIn.keySet()).containsExactlyInAnyOrder(SAGA_A, SAGA_B, SAGA_C);
      assertThat(bothIn.values())
          .as("the two nodes were in the step of every saga at once")
          .allMatch(latch -> latch.getCount() == 0);

      run.runToQuiescence(first);
      assertSameEnd(run.runToQuiescence(second), oneNode);
      assertThat(run.eventsOfSaga(SAGA_B)).containsOnlyOnce(new Undone(SAGA_B));
    }
  }

  /**
   * A node is paused between the end of its page and its checkpoint save; another node delivers
   * everything and saves the head. The paused save then lands late, and the deposed node stops
   * right after it (as its drain ends once its lease is gone): the monotonic guard keeps the
   * checkpoint at the head.
   */
  @Test
  void aLateCheckpointSaveOfADeposedNodeDoesNotMoveTheCheckpointBack() throws Exception {
    Scenario scenario = Scenario.base();
    Canon oneNode = uninterrupted(scenario);
    try (Run run = new Run(scenario)) {
      Node deposed = run.stores.node(Point.AFTER_SAVE_OFFSET, null);
      CountDownLatch atTheSave = new CountDownLatch(1);
      CountDownLatch resume = new CountDownLatch(1);
      AtomicBoolean paused = new AtomicBoolean();
      deposed.beforeCall =
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
      await(atTheSave, "the deposed node at its checkpoint save");
      assertThat(run.checkpoint()).as("its page ended, its save is pending").isZero();

      Canon byTheLeader = run.runToQuiescence(run.stores.node());
      long head = run.checkpoint();
      assertThat(head).isEqualTo((long) run.world.stored().size()).isGreaterThan(3L);

      resume.countDown();
      assertThat(late.join(AWAIT)).isTrue();
      assertThat(deposedOutcome.get()).isInstanceOf(NodeCrashed.class);
      assertThat(run.checkpoint()).as("the late save of offset 3 is a no-op").isEqualTo(head);
      assertSameEnd(byTheLeader, oneNode);
      assertSameEnd(run.canonicalEnd(), oneNode);
    }
  }

  /**
   * An {@link Error} on a worker leaves the listener after the step in flight ended, stops the page
   * and stops the subscription terminally; a fresh node delivers the page again and ends where an
   * uninterrupted run ends.
   */
  @Test
  void anErrorOnAWorkerStopsTheSubscription_andAFreshNodeFinishes() {
    Scenario scenario = Scenario.base();
    Canon uninterrupted = uninterrupted(scenario);
    try (Run run = new Run(scenario)) {
      run.stores.failOnce("load", SAGA_B, WorkerError::new);
      Node node = run.stores.node();
      CountDownLatch release = new CountDownLatch(1);
      holdTheStepOfA(node, release);
      node.onInjectedFailure =
          (method, sagaId) -> releaseWhenEnded(Thread.currentThread(), release);
      node.subscription.start();
      try {
        awaitUntil(
            () -> node.subscription.state() == SubscriptionLifecycleState.STOPPED,
            "the subscription's terminal stop");
      } finally {
        node.subscription.close();
      }
      assertThat(run.row(SAGA_A))
          .map(LoadedSaga::genesisApplied)
          .as("a's step finished")
          .hasValue(true);
      assertThat(run.row(SAGA_B)).as("b failed on its first statement").isEmpty();
      assertThat(run.row(SAGA_C)).as("the page started nothing after the Error").isEmpty();
      assertThat(run.checkpoint()).isZero();

      assertSameEnd(run.runToQuiescence(run.stores.node()), uninterrupted);
    }
  }

  /**
   * The timeout runner claims a saga whose step is in flight on a worker. The claim is deferred,
   * then waits for the step lock; the step's forward command is released during the wait, commits,
   * and the step writes its row; only then is the claim made — on the row the step wrote. The two
   * sagas whose steps had ended are claimed by the first poll.
   */
  @Test
  void aTimeoutClaimWhileAWorkerStepsTheSaga_waitsForTheStep() throws Exception {
    try (Run run = new Run(Scenario.base())) {
      Node node = run.stores.node();
      CountDownLatch release = new CountDownLatch(1);
      CountDownLatch bEntered = new CountDownLatch(1);
      node.beforeCall =
          (method, sagaId) -> {
            if (method.equals("Step") && SAGA_B.equals(sagaId)) {
              bEntered.countDown();
              await(release, "b's release");
            }
          };
      Thread delivering = Thread.ofVirtual().start(() -> run.drainQuietly(node));
      await(bEntered, "b's forward command");
      awaitUntil(
          () -> node.calls.contains("applyEvent a") && node.calls.contains("applyEvent c"),
          "the steps of a and c");

      var timeoutRunner =
          SagaTimeoutRunner.<ParState>builder()
              .decider(CrashingStores.withDeadline(orchestrator(), Duration.ofMinutes(1)))
              .sagaStore(node.sagaStore)
              .commandBus(node.bus)
              .sagaType(SAGA_TYPE)
              .clock(MutableClock.startingAt(Instant.now().plus(Duration.ofHours(1))))
              .sagaRunner(node.runner)
              .claimWait(AWAIT.multipliedBy(2))
              .build();
      for (int poll = 0; poll < SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING; poll++) {
        timeoutRunner.pollOnce();
      }
      assertThat(timeoutRunner.deferralsOf(SagaId.of(SAGA_B)))
          .as("each poll deferred b's claim")
          .isEqualTo(SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING);
      Thread waitingPoll = Thread.ofVirtual().start(timeoutRunner::pollOnce);
      awaitUntil(
          () -> node.runner.stepLock().waitingFor(SAGA_TYPE, SagaId.of(SAGA_B)) > 0,
          "the claim waiting for b's step");
      assertThat(node.calls).doesNotContain("claimCompensating b");

      release.countDown();
      assertThat(waitingPoll.join(AWAIT.multipliedBy(2))).isTrue();
      assertThat(delivering.join(AWAIT)).isTrue();
      run.runToQuiescence(run.stores.node());

      List<String> calls = node.calls;
      assertThat(calls.indexOf("applyEvent b"))
          .as("the claim is made after the step's row write")
          .isGreaterThanOrEqualTo(0)
          .isLessThan(calls.indexOf("claimCompensating b"));
      for (String sagaId : List.of(SAGA_A, SAGA_B, SAGA_C)) {
        long offset = List.of(SAGA_A, SAGA_B, SAGA_C).indexOf(sagaId) + 1L;
        assertThat(run.eventsOfSaga(sagaId))
            .as(
                sagaId
                    + ": its forward command was not refused, and one compensation came after it")
            .containsExactly(new Started(sagaId), new Stepped(sagaId, offset), new Undone(sagaId));
        assertThat(run.row(sagaId).orElseThrow().status())
            .as(sagaId)
            .isEqualTo(SagaStatus.COMPENSATED);
      }
    }
  }

  /**
   * An operator's replay feed of a saga that a worker is stepping waits for the step lock and then
   * reads the row the worker wrote: the start event it feeds is a duplicate.
   */
  @Test
  void aReplayFeedAndAWorkerOnOneSagaRunOneAfterTheOther() throws Exception {
    Scenario scenario = Scenario.base();
    Canon uninterrupted = uninterrupted(scenario);
    try (Run run = new Run(scenario)) {
      Node node = run.stores.node();
      CountDownLatch release = new CountDownLatch(1);
      CountDownLatch bEntered = new CountDownLatch(1);
      node.beforeCall =
          (method, sagaId) -> {
            if (method.equals("Step") && SAGA_B.equals(sagaId)) {
              bEntered.countDown();
              await(release, "b's release");
            }
          };
      Thread delivering = Thread.ofVirtual().start(() -> run.drainQuietly(node));
      await(bEntered, "b's forward command");

      EventEnvelope startOfB = run.world.stored().get(1);
      assertThat(startOfB.event()).isEqualTo(new Started(SAGA_B));
      AtomicReference<StepOutcome> fed = new AtomicReference<>();
      Thread feed =
          Thread.ofVirtual()
              .start(
                  () -> fed.set(node.runner.feedReplay(SagaId.of(SAGA_B), startOfB, true, false)));
      awaitUntil(
          () -> node.runner.stepLock().waitingFor(SAGA_TYPE, SagaId.of(SAGA_B)) > 0,
          "the feed waiting for b's step");

      release.countDown();
      assertThat(feed.join(AWAIT)).isTrue();
      assertThat(delivering.join(AWAIT)).isTrue();
      assertThat(fed.get()).isEqualTo(StepOutcome.SKIPPED_DEDUP);
      List<String> calls = List.copyOf(node.calls);
      int workersLoad = calls.indexOf("load b");
      int workersWrite = calls.indexOf("applyEvent b");
      assertThat(workersLoad).isGreaterThanOrEqualTo(0).isLessThan(workersWrite);
      assertThat(calls.subList(workersLoad + 1, workersWrite))
          .as("no read of b lands inside the worker's step")
          .doesNotContain("load b");

      assertSameEnd(run.runToQuiescence(run.stores.node()), uninterrupted);
    }
  }

  /**
   * The thread that drives a drain is interrupted while it waits for the workers. It keeps waiting
   * for the step in flight, which ends whole, then fails the delivery with its interrupt status
   * restored; the page is not acknowledged and the next cycle delivers it again.
   */
  @Test
  void theDeliveringThreadInterruptedWhileWaiting_thePageIsDeliveredAgain() throws Exception {
    Scenario scenario = Scenario.base();
    Canon uninterrupted = uninterrupted(scenario);
    try (Run run = new Run(scenario)) {
      Node node = run.stores.node();
      CountDownLatch release = new CountDownLatch(1);
      CountDownLatch aEntered = holdTheStepOfA(node, release);
      AtomicReference<Throwable> outcome = new AtomicReference<>();
      AtomicBoolean interruptStatus = new AtomicBoolean();
      Thread deliverer =
          Thread.ofPlatform()
              .start(
                  () -> {
                    try {
                      node.subscription.triggerImmediatePoll();
                    } catch (Throwable t) {
                      outcome.set(t);
                    }
                    interruptStatus.set(Thread.currentThread().isInterrupted());
                  });
      await(aEntered, "a's forward command");
      awaitUntil(() -> deliverer.getState() == Thread.State.WAITING, "the deliverer to wait");

      deliverer.interrupt();
      assertThat(deliverer.join(Duration.ofMillis(300)))
          .as("the deliverer keeps waiting for a's step")
          .isFalse();
      release.countDown();
      assertThat(deliverer.join(AWAIT)).isTrue();

      assertThat(outcome.get())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Interrupted while a page of saga steps was in flight");
      assertThat(interruptStatus).as("the interrupt status is restored").isTrue();
      assertThat(run.checkpoint()).as("the page is not acknowledged").isZero();
      assertThat(run.row(SAGA_A).orElseThrow().genesisApplied())
          .as("a's worker was not interrupted: its step ended whole")
          .isTrue();
      assertThat(run.eventsOfSaga(SAGA_A)).contains(new Stepped(SAGA_A, 1));

      assertSameEnd(run.runToQuiescence(node), uninterrupted);
    }
  }

  // ==================== scenarios ====================

  /**
   * What a run does: the page it appends, the events that are poison, the sagas whose step is
   * rejected and how many compensation commands they dispatch, the calls that fail once (in the
   * crashed run only: the reference meets no failure), the saga the crash is on, whether the held
   * step of {@code a} is released once the page has decided what it started rather than at the
   * crash, and what the uninterrupted run ends in.
   */
  private record Scenario(
      List<DomainEvent> page,
      Set<Long> poisonOffsets,
      Set<String> rejecting,
      int compensationCommands,
      List<Failing> failingOnce,
      String crashOn,
      boolean releaseAWhenBEnded,
      Map<String, SagaStatus> expectedStatus,
      Set<String> expectedDeadLetters) {

    /** The {@code nthCall}-th call of {@code method} for {@code sagaId} fails once. */
    record Failing(String method, String sagaId, int nthCall, Supplier<RuntimeException> failure) {}

    static Builder builder() {
      return new Builder();
    }

    static Scenario base() {
      return builder().build();
    }

    static Scenario of(Point point) {
      return switch (point) {
        case BEFORE_FIRST_STATEMENT,
            AFTER_GENESIS_PENDING,
            AFTER_FORWARD_COMMAND_COMMIT,
            AFTER_ROW_WRITE ->
            base();
        case BETWEEN_EVENTS_OF_ONE_SAGA ->
            builder()
                .page(
                    new Started(SAGA_A),
                    new Started(SAGA_B),
                    new Replied(SAGA_B),
                    new Replied(SAGA_B),
                    new Started(SAGA_C))
                .build();
        case AFTER_COMPENSATION_CLAIM, AFTER_FIRST_COMPENSATION_COMMAND, BEFORE_TERMINAL_WRITE ->
            builder()
                .rejecting(SAGA_B)
                .compensationCommands(2)
                .ending(SAGA_B, SagaStatus.COMPENSATED)
                .build();
        case BEFORE_POISON_ENTRY, AFTER_POISON_ENTRY, AFTER_FAULT_WRITE ->
            builder()
                .page(
                    new Started(SAGA_A),
                    new Started(SAGA_B),
                    new Replied(SAGA_B),
                    new Started(SAGA_C))
                .poison(3)
                .ending(SAGA_B, SagaStatus.FAULTED)
                .deadLetters(SAGA_B)
                .build();
        case BEFORE_HOLD_ENTRY, AFTER_HOLD_ENTRY ->
            builder()
                .page(
                    new Started(SAGA_A),
                    new Started(SAGA_B),
                    new Replied(SAGA_B),
                    new Replied(SAGA_B),
                    new Started(SAGA_C))
                .poison(3)
                .ending(SAGA_B, SagaStatus.FAULTED)
                .deadLetters(SAGA_B)
                .build();
        case AFTER_NULL_SAGA_ENTRY ->
            builder()
                .page(
                    new Started(SAGA_A),
                    new Started(SAGA_B),
                    new Replied(UNROUTABLE),
                    new Started(SAGA_C))
                .crashOn(null)
                .deadLetters(NULL_SAGA)
                .build();
        case AFTER_FAILED_PAGE_THREW ->
            // b's second event fails; its third must not be started in the failed page
            builder()
                .page(
                    new Started(SAGA_A),
                    new Started(SAGA_B),
                    new Replied(SAGA_B),
                    new Replied(SAGA_B),
                    new Started(SAGA_C))
                .failingOnce("load", SAGA_B, 2, SagaParallelCrashPointTest::statementTimeout)
                .releasingAWhenBEnded()
                .build();
        case AFTER_PAGE_STOP ->
            builder()
                .failingOnce("load", SAGA_B, 1, SagaParallelCrashPointTest::connectionLost)
                .releasingAWhenBEnded()
                .build();
        case AFTER_THIRD_FAILURE_STOP ->
            // b, c and d fail on one worker while a is held; the third failure stops the page
            builder()
                .page(
                    new Started(SAGA_A),
                    new Started(SAGA_B),
                    new Started(SAGA_C),
                    new Started(SAGA_D),
                    new Started(SAGA_E))
                .failingOnce("load", SAGA_B, 1, SagaParallelCrashPointTest::statementTimeout)
                .failingOnce("load", SAGA_C, 1, SagaParallelCrashPointTest::statementTimeout)
                .failingOnce("load", SAGA_D, 1, SagaParallelCrashPointTest::statementTimeout)
                .releasingAWhenBEnded()
                .build();
        case AFTER_SECOND_FAILURE ->
            // b fails before its first statement, c after its command committed; d still runs
            builder()
                .page(
                    new Started(SAGA_A),
                    new Started(SAGA_B),
                    new Started(SAGA_C),
                    new Started(SAGA_D))
                .failingOnce("load", SAGA_B, 1, SagaParallelCrashPointTest::statementTimeout)
                .failingOnce("applyEvent", SAGA_C, 1, SagaParallelCrashPointTest::statementTimeout)
                .releasingAWhenBEnded()
                .build();
        case WHILE_FAILED_PAGE_RUNS_OTHERS ->
            builder()
                .failingOnce("load", SAGA_B, 1, SagaParallelCrashPointTest::statementTimeout)
                .crashOn(SAGA_C)
                .build();
        case BEFORE_SAVE_OFFSET, AFTER_SAVE_OFFSET -> builder().releasingAWhenBEnded().build();
      };
    }

    /** This scenario with nothing failing: the reference of an uninterrupted run. */
    Scenario withoutFailures() {
      Builder builder = toBuilder();
      builder.failing.clear();
      return builder.build();
    }

    Builder toBuilder() {
      Builder builder = new Builder();
      builder.page = page;
      builder.poisonOffsets = poisonOffsets;
      builder.rejecting = rejecting;
      builder.compensationCommands = compensationCommands;
      builder.failing.addAll(failingOnce);
      builder.crashOn = crashOn;
      builder.releaseAWhenBEnded = releaseAWhenBEnded;
      builder.ending.putAll(expectedStatus);
      builder.deadLetters = expectedDeadLetters;
      return builder;
    }

    /**
     * A scenario, by default the page {@code a}, {@code b}, {@code c} with the crash on {@code b}.
     */
    static final class Builder {
      private List<DomainEvent> page =
          List.of(new Started(SAGA_A), new Started(SAGA_B), new Started(SAGA_C));
      private Set<Long> poisonOffsets = Set.of();
      private Set<String> rejecting = Set.of();
      private int compensationCommands = 1;
      private final List<Failing> failing = new ArrayList<>();
      private String crashOn = SAGA_B;
      private boolean releaseAWhenBEnded;
      private final Map<String, SagaStatus> ending = new HashMap<>();
      private Set<String> deadLetters = Set.of();

      Builder page(DomainEvent... events) {
        page = List.of(events);
        return this;
      }

      Builder poison(long offset) {
        poisonOffsets = Set.of(offset);
        return this;
      }

      Builder rejecting(String sagaId) {
        rejecting = Set.of(sagaId);
        return this;
      }

      Builder compensationCommands(int count) {
        compensationCommands = count;
        return this;
      }

      Builder failingOnce(
          String method, String sagaId, int nthCall, Supplier<RuntimeException> failure) {
        failing.add(new Failing(method, sagaId, nthCall, failure));
        return this;
      }

      Builder crashOn(String sagaId) {
        crashOn = sagaId;
        return this;
      }

      Builder releasingAWhenBEnded() {
        releaseAWhenBEnded = true;
        return this;
      }

      /** The saga ends in {@code status}; a saga started by the page ends RUNNING otherwise. */
      Builder ending(String sagaId, SagaStatus status) {
        ending.put(sagaId, status);
        return this;
      }

      Builder deadLetters(String... sagaIds) {
        deadLetters = Set.of(sagaIds);
        return this;
      }

      Scenario build() {
        Map<String, SagaStatus> expected = new TreeMap<>();
        for (DomainEvent event : page) {
          if (event instanceof Started started) {
            expected.put(started.id(), SagaStatus.RUNNING);
          }
        }
        expected.putAll(ending);
        return new Scenario(
            page,
            poisonOffsets,
            rejecting,
            compensationCommands,
            List.copyOf(failing),
            crashOn,
            releaseAWhenBEnded,
            Map.copyOf(expected),
            deadLetters);
      }
    }

    /**
     * The saga of this scenario: poison at its offsets, an event no saga is routed for, and its
     * number of compensation commands.
     */
    SagaOrchestrator<ParState> saga() {
      return new Delegating(orchestrator()) {
        @Override
        public ParState evolve(ParState state, EventEnvelope event) {
          if (poisonOffsets.contains(event.globalOffset().value())) {
            throw new IllegalStateException("poison at " + event.globalOffset().value());
          }
          return super.evolve(state, event);
        }

        @Override
        public Optional<SagaId> correlateByEventData(StreamId streamId, DomainEvent event) {
          if (event instanceof Replied replied && replied.id().equals(UNROUTABLE)) {
            throw new IllegalStateException("no saga for " + replied.id());
          }
          return super.correlateByEventData(streamId, event);
        }

        @Override
        public List<SagaCommand> compensate(
            ParState state, Throwable failure, SagaCommand failedCommand) {
          return Collections.nCopies(
              compensationCommands,
              SagaCommand.of(new Undo(state.sagaId()), aggregateOf(state.sagaId())));
        }
      };
    }

    /**
     * The uninterrupted run ends as the scenario says: every event delivered, every saga in its
     * status, a running saga with every event of its own applied, and the dead letters it names.
     */
    void assertEndsAsExpected(Canon end, World world) {
      assertThat(end.checkpoint()).as("every event delivered").isEqualTo(world.stored().size());
      assertThat(end.rowsBySaga().keySet()).isEqualTo(expectedStatus.keySet());
      expectedStatus.forEach(
          (sagaId, status) -> {
            CanonRow row = end.rowsBySaga().get(sagaId);
            assertThat(row.status()).as(sagaId).isEqualTo(status);
            if (status == SagaStatus.RUNNING) {
              assertThat(row.genesisApplied()).as(sagaId).isTrue();
              long own =
                  world.stored().stream().filter(e -> sagaId.equals(idOf(e.event()))).count();
              assertThat(row.applied()).as(sagaId + " applied").hasSize((int) own);
            }
          });
      assertThat(end.deadLettersBySaga().keySet()).isEqualTo(expectedDeadLetters);
    }

    /** What the crash left, so a case is known to have stopped where it says. */
    void assertStateLeft(Point point, Run run) {
      if (point != Point.AFTER_SAVE_OFFSET) {
        assertThat(run.checkpoint()).as("the crashed page is not acknowledged").isZero();
      }
      switch (point) {
        case BEFORE_FIRST_STATEMENT -> {
          assertThat(run.row(SAGA_B)).isEmpty();
          assertMidStepAndNotStarted(run);
        }
        case AFTER_GENESIS_PENDING -> {
          assertGenesisPending(run, SAGA_B);
          assertThat(run.eventsOfSaga(SAGA_B)).doesNotContain(new Stepped(SAGA_B, 2));
          assertMidStepAndNotStarted(run);
        }
        case AFTER_FORWARD_COMMAND_COMMIT -> {
          assertGenesisPending(run, SAGA_B);
          assertThat(run.eventsOfSaga(SAGA_B)).contains(new Stepped(SAGA_B, 2));
          assertMidStepAndNotStarted(run);
        }
        case AFTER_ROW_WRITE -> {
          assertThat(run.row(SAGA_B).orElseThrow().genesisApplied()).isTrue();
          assertMidStepAndNotStarted(run);
        }
        case BETWEEN_EVENTS_OF_ONE_SAGA -> {
          assertThat(run.row(SAGA_B).orElseThrow().state().applied()).containsExactly(2L, 3L);
          assertMidStepAndNotStarted(run);
        }
        case AFTER_COMPENSATION_CLAIM -> {
          assertThat(run.row(SAGA_B).orElseThrow().status()).isEqualTo(SagaStatus.COMPENSATING);
          assertThat(run.eventsOfSaga(SAGA_B)).doesNotContain(new Undone(SAGA_B));
          assertMidStepAndNotStarted(run);
        }
        case AFTER_FIRST_COMPENSATION_COMMAND -> {
          assertThat(run.row(SAGA_B).orElseThrow().status()).isEqualTo(SagaStatus.COMPENSATING);
          assertThat(run.eventsOfSaga(SAGA_B))
              .as("between the two compensation commands")
              .containsOnlyOnce(new Undone(SAGA_B));
          assertMidStepAndNotStarted(run);
        }
        case BEFORE_TERMINAL_WRITE -> {
          assertThat(run.row(SAGA_B).orElseThrow().status()).isEqualTo(SagaStatus.COMPENSATING);
          assertThat(run.eventsOfSaga(SAGA_B).stream().filter(new Undone(SAGA_B)::equals))
              .as("both compensation commands committed")
              .hasSize(2);
          assertMidStepAndNotStarted(run);
        }
        case BEFORE_POISON_ENTRY -> {
          assertThat(run.row(SAGA_B).orElseThrow().status()).isEqualTo(SagaStatus.RUNNING);
          assertThat(run.row(SAGA_B).orElseThrow().deadLetterPending()).isFalse();
          assertThat(run.world.deadLetters.all()).isEmpty();
          assertMidStepAndNotStarted(run);
        }
        case AFTER_POISON_ENTRY -> {
          assertThat(run.row(SAGA_B).orElseThrow().status()).isEqualTo(SagaStatus.RUNNING);
          assertThat(run.row(SAGA_B).orElseThrow().deadLetterPending()).isTrue();
          assertThat(run.deadLetterOffsets(SAGA_B)).containsExactly(3L);
          assertMidStepAndNotStarted(run);
        }
        case AFTER_FAULT_WRITE, BEFORE_HOLD_ENTRY -> {
          assertThat(run.row(SAGA_B).orElseThrow().status()).isEqualTo(SagaStatus.FAULTED);
          assertThat(run.deadLetterOffsets(SAGA_B)).containsExactly(3L);
          assertMidStepAndNotStarted(run);
        }
        case AFTER_HOLD_ENTRY -> {
          assertThat(run.row(SAGA_B).orElseThrow().status()).isEqualTo(SagaStatus.FAULTED);
          assertThat(run.deadLetterOffsets(SAGA_B)).containsExactly(3L, 4L);
          assertMidStepAndNotStarted(run);
        }
        case AFTER_NULL_SAGA_ENTRY -> {
          assertThat(run.world.deadLetters.all()).hasSize(1);
          assertThat(run.world.rows.all()).as("no step started").isEmpty();
        }
        case AFTER_FAILED_PAGE_THREW -> {
          assertThat(run.row(SAGA_B).orElseThrow().state().applied())
              .as("b's first event is applied, its second failed")
              .containsExactly(2L);
          assertThat(run.crashedNode.calls.stream().filter("load b"::equals))
              .as("nothing of b was started after its failure")
              .hasSize(1);
          assertThat(run.row(SAGA_A).orElseThrow().genesisApplied()).isTrue();
          assertThat(run.row(SAGA_C).orElseThrow().genesisApplied())
              .as("the page ran on after b's failure")
              .isTrue();
        }
        case AFTER_PAGE_STOP -> {
          assertThat(run.row(SAGA_B)).isEmpty();
          assertThat(run.row(SAGA_A).orElseThrow().genesisApplied())
              .as("the step in flight finished")
              .isTrue();
          assertThat(run.row(SAGA_C)).as("the stopped page started nothing more").isEmpty();
        }
        case AFTER_THIRD_FAILURE_STOP -> {
          for (String failed : List.of(SAGA_B, SAGA_C, SAGA_D)) {
            assertThat(run.row(failed)).as(failed + " failed").isEmpty();
          }
          assertThat(run.row(SAGA_A).orElseThrow().genesisApplied())
              .as("the step in flight finished")
              .isTrue();
          assertThat(run.crashedNode.calls)
              .as("the third failure stopped the page: e was not started")
              .noneMatch(call -> call.endsWith(" " + SAGA_E));
        }
        case AFTER_SECOND_FAILURE -> {
          assertThat(run.row(SAGA_B)).isEmpty();
          assertGenesisPending(run, SAGA_C);
          assertThat(run.eventsOfSaga(SAGA_C)).contains(new Stepped(SAGA_C, 3));
          assertThat(run.row(SAGA_D).map(LoadedSaga::genesisApplied))
              .as("two failures do not stop the page")
              .hasValue(true);
          assertThat(run.row(SAGA_A).orElseThrow().genesisApplied()).isTrue();
        }
        case WHILE_FAILED_PAGE_RUNS_OTHERS -> {
          assertThat(run.row(SAGA_B)).isEmpty();
          assertThat(run.row(SAGA_C).orElseThrow().genesisApplied()).isTrue();
          assertGenesisPending(run, SAGA_A);
        }
        case BEFORE_SAVE_OFFSET ->
            assertThat(run.world.rows.all()).hasSize(3).allSatisfy(id -> assertApplied(run, id));
        case AFTER_SAVE_OFFSET -> {
          assertThat(run.checkpoint()).as("the page's checkpoint is saved").isEqualTo(3);
          assertThat(run.world.rows.all()).hasSize(3).allSatisfy(id -> assertApplied(run, id));
        }
      }
    }

    private static void assertApplied(Run run, SagaId sagaId) {
      assertThat(run.row(sagaId.value()).orElseThrow().genesisApplied())
          .as(sagaId.value())
          .isTrue();
    }

    private static void assertGenesisPending(Run run, String sagaId) {
      LoadedSaga<ParState> row = run.row(sagaId).orElseThrow();
      assertThat(row.genesisApplied()).as(sagaId + " genesis applied").isFalse();
      assertThat(row.version()).as(sagaId + " version").isEqualTo(1L);
    }

    /** {@code a} was cut mid-step (its row written, its command not), {@code c} never started. */
    private static void assertMidStepAndNotStarted(Run run) {
      assertGenesisPending(run, SAGA_A);
      assertThat(run.eventsOfSaga(SAGA_A)).containsExactly(new Started(SAGA_A));
      assertThat(run.row(SAGA_C)).isEmpty();
    }
  }

  // ==================== the runs ====================

  /** A world with the scenario's page appended, and the stores its nodes run over. */
  private static final class Run implements AutoCloseable {
    final World world = new World();
    final Scenario scenario;
    final CrashingStores stores;
    Node crashedNode;

    Run(Scenario scenario) {
      this.scenario = scenario;
      world.rejecting.addAll(scenario.rejecting());
      stores = new CrashingStores(world, scenario.saga(), PARALLELISM);
      scenario
          .failingOnce()
          .forEach(f -> stores.failOnce(f.method(), f.sagaId(), f.nthCall(), f.failure()));
      world.append(scenario.page().toArray(DomainEvent[]::new));
    }

    /**
     * Runs a node that stops at {@code point}: {@code a}'s forward command is held until the crash
     * — or, where the page has to end or fail first, until {@code b}'s row write, or until the
     * worker of the page's first failure has ended — and {@code b} starts only once {@code a} is
     * held. A failed page is delivered again, as the poll loop does after its backoff, until the
     * node crashes.
     */
    void runUntilCrash(Point point) {
      Node node = stores.node(point, scenario.crashOn());
      crashedNode = node;
      CountDownLatch release = new CountDownLatch(1);
      CountDownLatch aEntered = holdTheStepOfA(node, release);
      var holdA = node.beforeCall;
      node.beforeCall =
          (method, sagaId) -> {
            holdA.accept(method, sagaId);
            if (SAGA_B.equals(sagaId)) {
              await(aEntered, "a's forward command, before b starts");
            }
          };
      node.onCrash = release::countDown;
      if (scenario.releaseAWhenBEnded()) {
        if (scenario.failingOnce().isEmpty()) {
          node.afterCall =
              (method, sagaId) -> {
                if (SAGA_B.equals(sagaId) && method.equals("applyEvent")) {
                  release.countDown();
                }
              };
        } else {
          AtomicBoolean watching = new AtomicBoolean();
          node.onInjectedFailure =
              (method, sagaId) -> {
                // the worker of the first failure runs what the page still starts, then ends: a is
                // released only then, so what the page started is decided, not raced
                if (watching.compareAndSet(false, true)) {
                  releaseWhenEnded(Thread.currentThread(), release);
                }
              };
        }
      }
      for (int cycle = 0; cycle < MAX_CYCLES && !node.crashed(); cycle++) {
        try {
          node.cycle();
        } catch (NodeCrashed crashed) {
          break;
        } catch (RuntimeException failedPage) {
          // delivered again on the next cycle
        }
      }
      assertThat(node.crashed()).as("the node reached " + point).isTrue();
    }

    /** Runs cycles of the node until one delivers nothing; returns the end state. */
    Canon runToQuiescence(Node node) {
      RuntimeException last = null;
      for (int cycle = 0; cycle < MAX_CYCLES; cycle++) {
        try {
          if (node.cycle() == 0) {
            assertThat(checkpoint()).as("every event delivered").isEqualTo(world.stored().size());
            return canonicalEnd();
          }
        } catch (RuntimeException failedPage) {
          last = failedPage;
        }
      }
      throw new AssertionError("no quiescence after " + MAX_CYCLES + " cycles", last);
    }

    /** Runs cycles of the node until one delivers nothing, ignoring a failure of the node. */
    void drainQuietly(Node node) {
      for (int cycle = 0; cycle < MAX_CYCLES; cycle++) {
        try {
          if (node.cycle() == 0) {
            return;
          }
        } catch (RuntimeException failedPage) {
          // delivered again on the next cycle
        }
      }
    }

    long checkpoint() {
      return world.offsets.getLastOffset(CHECKPOINT).value();
    }

    Optional<LoadedSaga<ParState>> row(String sagaId) {
      return world.rows.load(SagaId.of(sagaId), SAGA_TYPE, ParState.class);
    }

    /** The events that name the saga, in global order. */
    List<DomainEvent> eventsOfSaga(String sagaId) {
      return world.stored().stream()
          .map(EventEnvelope::event)
          .filter(e -> sagaId.equals(idOf(e)))
          .toList();
    }

    List<Long> deadLetterOffsets(String sagaId) {
      return world.deadLetters.all().stream()
          .filter(e -> e.sagaId() != null && e.sagaId().value().equals(sagaId))
          .map(e -> e.eventOffset().value())
          .sorted()
          .toList();
    }

    Canon canonicalEnd() {
      return canonical(world);
    }

    @Override
    public void close() {
      world.close();
    }
  }

  private static Canon uninterrupted(Scenario scenario) {
    try (Run run = new Run(scenario.withoutFailures())) {
      Canon end = run.runToQuiescence(run.stores.node());
      scenario.assertEndsAsExpected(end, run.world);
      return end;
    }
  }

  /** The two end states are equal; the stored events first, as every other part names them. */
  private static void assertSameEnd(Canon actual, Canon expected) {
    assertThat(actual.eventsByAggregate()).isEqualTo(expected.eventsByAggregate());
    assertThat(actual.rowsBySaga()).isEqualTo(expected.rowsBySaga());
    assertThat(actual.inboxKeys()).isEqualTo(expected.inboxKeys());
    assertThat(actual.deadLettersBySaga()).isEqualTo(expected.deadLettersBySaga());
    assertThat(actual.checkpoint()).isEqualTo(expected.checkpoint());
    assertThat(actual.commandsBySaga()).isEqualTo(expected.commandsBySaga());
  }

  /**
   * Holds the first forward command of {@code a} until {@code release} opens; the returned latch
   * opens when it is held.
   */
  private static CountDownLatch holdTheStepOfA(Node node, CountDownLatch release) {
    CountDownLatch entered = new CountDownLatch(1);
    AtomicBoolean held = new AtomicBoolean();
    node.beforeCall =
        (method, sagaId) -> {
          if (method.equals("Step") && SAGA_A.equals(sagaId) && held.compareAndSet(false, true)) {
            entered.countDown();
            await(release, "a's release");
          }
        };
    return entered;
  }

  /** Opens {@code release} once {@code worker} has ended. */
  private static void releaseWhenEnded(Thread worker, CountDownLatch release) {
    Thread.ofVirtual()
        .start(
            () -> {
              try {
                worker.join();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
              release.countDown();
            });
  }

  /**
   * What a run left behind, with every recorded offset given as the event it names ({@code
   * stream@version}).
   *
   * @param eventsByAggregate the events of each stream, in order
   * @param rowsBySaga each saga's row, by saga id
   * @param inboxKeys every key a command was dispatched under that the inbox recorded
   * @param deadLettersBySaga each saga's dead-letter entries; the null-saga entries under {@link
   *     SagaParallelFixtures#NULL_SAGA}
   * @param checkpoint the subscription's saved offset
   * @param commandsBySaga the commands dispatched for each saga, in order, repeats removed
   */
  record Canon(
      Map<StreamId, List<DomainEvent>> eventsByAggregate,
      Map<String, CanonRow> rowsBySaga,
      Set<IdempotencyKey> inboxKeys,
      Map<String, List<CanonDeadLetter>> deadLettersBySaga,
      long checkpoint,
      Map<String, List<Command>> commandsBySaga) {}

  /** The parts of a saga row a run decides, its offsets as the events they name. */
  record CanonRow(
      SagaStatus status,
      long version,
      boolean genesisApplied,
      boolean deadLetterPending,
      SagaStatus preFaultStatus,
      Long episodeVersion,
      String lastApplied,
      SagaStatus stateStatus,
      String stateSagaId,
      List<String> applied) {}

  /** A dead-letter entry: the event it records and the class name of the error. */
  record CanonDeadLetter(String event, String errorType) {}

  /** The end state of a world, each recorded offset given as the event it names. */
  static Canon canonical(World world) {
    List<EventEnvelope> stored = world.stored();
    Map<Long, String> names = new HashMap<>();
    Map<StreamId, List<DomainEvent>> events = new LinkedHashMap<>();
    for (EventEnvelope e : stored) {
      names.put(
          e.globalOffset().value(),
          e.streamId().aggregateType().value()
              + "/"
              + e.streamId().aggregateId().value()
              + "@"
              + e.version().value());
      events.computeIfAbsent(e.streamId(), s -> new ArrayList<>()).add(e.event());
    }
    java.util.function.LongFunction<String> named =
        offset ->
            Objects.requireNonNull(
                names.get(offset), "a recorded offset names no stored event: " + offset);
    Map<String, CanonRow> rows = new TreeMap<>();
    for (SagaId id : world.rows.all()) {
      LoadedSaga<ParState> row = world.rows.load(id, SAGA_TYPE, ParState.class).orElseThrow();
      rows.put(
          id.value(),
          new CanonRow(
              row.status(),
              row.version(),
              row.genesisApplied(),
              row.deadLetterPending(),
              row.preFaultStatus(),
              row.episodeVersion(),
              row.lastAppliedOffset() == null ? null : named.apply(row.lastAppliedOffset()),
              row.state().status(),
              row.state().sagaId(),
              row.state().applied().stream().map(named::apply).toList()));
    }
    Map<String, List<CanonDeadLetter>> deadLetters = new TreeMap<>();
    for (SagaDeadLetterEntry entry : world.deadLetters.all()) {
      String saga = entry.sagaId() == null ? NULL_SAGA : entry.sagaId().value();
      deadLetters
          .computeIfAbsent(saga, s -> new ArrayList<>())
          .add(new CanonDeadLetter(named.apply(entry.eventOffset().value()), entry.errorType()));
    }
    deadLetters.values().forEach(l -> l.sort(Comparator.comparing(CanonDeadLetter::event)));
    EndState raw = world.endState(CHECKPOINT);
    Map<String, List<Command>> commands = new TreeMap<>();
    raw.commandsBySaga()
        .forEach((saga, list) -> commands.put(saga, List.copyOf(new LinkedHashSet<>(list))));
    return new Canon(events, rows, raw.inboxKeys(), deadLetters, raw.checkpoint(), commands);
  }

  private static RuntimeException statementTimeout() {
    return new IllegalStateException("canceling statement due to statement timeout");
  }

  private static RuntimeException connectionLost() {
    return new IllegalStateException(
        "the connection is gone", new SQLTransientConnectionException("connection lost"));
  }

  /** An {@link Error} a worker's step throws. */
  private static final class WorkerError extends Error {
    WorkerError() {
      super("a worker's step threw an Error");
    }
  }

  private static void awaitUntil(BooleanSupplier condition, String what) {
    long deadline = System.nanoTime() + AWAIT.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("timed out waiting for " + what);
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError("interrupted waiting for " + what, e);
      }
    }
  }
}
