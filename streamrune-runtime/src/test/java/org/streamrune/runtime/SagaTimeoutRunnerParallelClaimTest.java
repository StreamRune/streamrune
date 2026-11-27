package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.SagaStepLockFixture.TIMEOUT;
import static org.streamrune.runtime.SagaStepLockFixture.await;
import static org.streamrune.runtime.SagaStepLockFixture.awaitUntil;
import static org.streamrune.runtime.SagaStepLockFixture.onAnotherThread;
import static org.streamrune.runtime.SagaStepLockFixture.outcomeOf;
import static org.streamrune.runtime.SagaStepLockFixture.park;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.runtime.SagaStepLockFixture.Advanced;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.Refund;
import org.streamrune.runtime.SagaStepLockFixture.Release;
import org.streamrune.runtime.SagaStepLockFixture.Reserve;
import org.streamrune.runtime.SagaStepLockFixture.Ship;
import org.streamrune.runtime.SagaStepLockFixture.State;
import org.streamrune.test.InMemoryEventSubscription;

/**
 * The timeout runner claims the sagas of a query in parallel, on the slots of the saga runner it
 * was built from: the runner's steps and its claims together never exceed the runner's budgeted
 * steps, a slow compensation delays no other claim, a slot is taken before the step lock, and a
 * page delivered on a thread that holds a slot runs on that thread.
 */
@Timeout(60)
class SagaTimeoutRunnerParallelClaimTest {

  private static void parallelism(SagaStepLockFixture p, int parallelism) {
    p.runner.applyParallelismSettings(new SagaParallelismSettings(parallelism, Map.of()));
  }

  private static boolean compensated(SagaStepLockFixture p, String... orders) {
    for (String order : orders) {
      if (p.row(order).status() != SagaStatus.COMPENSATED) {
        return false;
      }
    }
    return true;
  }

  /** Opens the sagas of {@code orders} one second apart, so the store orders them so. */
  private static void open(SagaStepLockFixture p, String... orders) {
    for (String order : orders) {
      p.deliver(p.append(new Opened(order)));
      p.clock.advance(Duration.ofSeconds(1));
    }
  }

  /**
   * Starts a delivery of the saga of each order on its own thread whose forward command does not
   * return until {@link #returnAtLast()}.
   */
  private static final class HungSteps {
    private final CountDownLatch release = new CountDownLatch(1);
    private final List<CompletableFuture<Void>> deliveries = new ArrayList<>();

    HungSteps(SagaStepLockFixture p, String... orders) {
      Set<String> parked = Set.of(orders);
      var reached = new CountDownLatch(orders.length);
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Reserve reserve && parked.contains(reserve.orderId())) {
              reached.countDown();
              park(release);
            }
          };
      for (String order : orders) {
        var opened = p.append(new Opened(order));
        deliveries.add(onAnotherThread(() -> p.deliver(opened)));
        p.awaitRow(order);
        p.clock.advance(Duration.ofSeconds(1));
      }
      await(reached);
    }

    void returnAtLast() {
      release.countDown();
      for (CompletableFuture<Void> delivery : deliveries) {
        assertThat(outcomeOf(delivery)).isNull();
      }
    }
  }

  @Test
  void aSlowCompensation_doesNotDelayTheClaimsOfTheOtherSagas() {
    try (var p = new SagaStepLockFixture()) {
      open(p, "1", "2", "3", "4");
      var slow = new CountDownLatch(1);
      var reached = new CountDownLatch(1);
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Release release && release.orderId().equals("1")) {
              reached.countDown();
              park(slow);
            }
          };
      p.clock.advance(TIMEOUT);

      CompletableFuture<Void> poll = onAnotherThread(p.timeoutRunner::pollOnce);
      await(reached);
      awaitUntil(
          "the other sagas are compensated while the first one's compensation hangs",
          () -> compensated(p, "2", "3", "4"));
      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(poll).isNotDone();

      slow.countDown();
      assertThat(outcomeOf(poll)).isNull();
      assertThat(compensated(p, "1", "2", "3", "4")).isTrue();
      assertThat(p.runner.slots().inUse()).isZero();
    }
  }

  @Test
  void atParallelismOne_theClaimsRunOneAfterTheOther_onThePollingThread() {
    try (var p = new SagaStepLockFixture()) {
      parallelism(p, 1);
      open(p, "1", "2", "3");
      var releases = new CopyOnWriteArrayList<String>();
      var threads = ConcurrentHashMap.<Thread>newKeySet();
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Release release) {
              releases.add(release.orderId());
              threads.add(Thread.currentThread());
            }
          };
      p.clock.advance(TIMEOUT);

      p.timeoutRunner.pollOnce();

      assertThat(releases).containsExactly("1", "2", "3");
      assertThat(threads).containsExactly(Thread.currentThread());
    }
  }

  @Test
  void stepsAndClaimsTogether_neverExceedTheBudgetedSteps() {
    try (var p = new SagaStepLockFixture()) {
      parallelism(p, 2);
      open(p, "1", "2", "3", "4");
      p.clock.advance(TIMEOUT);
      var dispatching = new AtomicInteger();
      var most = new AtomicInteger();
      var slotsSeen = new AtomicInteger();
      // The first two commands wait for each other, so two dispatch at once whatever the timing.
      var firstTwo = new CountDownLatch(2);
      p.bus.beforeEveryCommand =
          command -> {
            most.accumulateAndGet(dispatching.incrementAndGet(), Math::max);
            slotsSeen.accumulateAndGet(p.runner.slots().inUse(), Math::max);
            try {
              if (firstTwo.getCount() > 0) {
                firstTwo.countDown();
                await(firstTwo);
              }
              Thread.sleep(20);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            } finally {
              dispatching.decrementAndGet();
            }
          };
      List<EventEnvelope> page = new ArrayList<>();
      for (String order : List.of("5", "6", "7", "8")) {
        page.add(p.append(new Opened(order)));
      }

      CompletableFuture<Void> steps =
          onAnotherThread(() -> p.runner.asEventListener().onEvents(page));
      CompletableFuture<Void> claims = onAnotherThread(p.timeoutRunner::pollOnce);

      assertThat(outcomeOf(steps)).isNull();
      assertThat(outcomeOf(claims)).isNull();
      assertThat(most.get()).as("steps and claims dispatching at once").isEqualTo(2);
      assertThat(slotsSeen.get()).isLessThanOrEqualTo(2);
      assertThat(compensated(p, "1", "2", "3", "4")).isTrue();
      for (String order : List.of("5", "6", "7", "8")) {
        assertThat(p.storedOrder(order)).containsExactly("Reserved");
      }
    }
  }

  @Test
  void aRaisedParallelism_admitsAClaimThatWaitsForASlotHeldByAStep() {
    try (var p = new SagaStepLockFixture()) {
      parallelism(p, 1);
      open(p, "2"); // before the step that holds the only slot
      var hung = new HungSteps(p, "1");
      p.clock.advance(TIMEOUT);

      CompletableFuture<Void> poll = onAnotherThread(p.timeoutRunner::pollOnce);
      awaitUntil(
          "the claim waits for the slot the step holds", () -> p.runner.slots().waiting() == 1);
      assertThat(p.row("2").status()).isEqualTo(SagaStatus.RUNNING);

      parallelism(p, 2);

      assertThat(outcomeOf(poll)).isNull();
      assertThat(p.row("2").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.row("1").episodeVersion())
          .as("the saga in a step is passed over: its step still holds its lock")
          .isNull();
      assertThat(p.timeoutRunner.deferralsOf(SagaStepLockFixture.sagaOf("1"))).isEqualTo(1);
      hung.returnAtLast();
    }
  }

  @Test
  void aClaimThatWaitsForAStepLock_whileThatStepsPageNeedsASlot_doesNotDeadlock() {
    try (var p = new SagaStepLockFixture(new SagaStepLock(), Duration.ofSeconds(20))) {
      parallelism(p, 2);
      var release = new CountDownLatch(1);
      var reached = new CountDownLatch(1);
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Reserve reserve && reserve.orderId().equals("1")) {
              reached.countDown();
              park(release);
            }
          };
      // One page: the start of saga 1, which hangs, then its next event, then another saga.
      List<EventEnvelope> page =
          List.of(
              p.append(new Opened("1")), p.append(new Advanced("1")), p.append(new Opened("3")));
      CompletableFuture<Void> delivery =
          onAnotherThread(() -> p.runner.asEventListener().onEvents(page));
      await(reached);
      p.clock.advance(TIMEOUT.plusSeconds(1));
      for (int poll = 0; poll < SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING; poll++) {
        p.timeoutRunner.pollOnce();
      }

      long start = System.nanoTime();
      CompletableFuture<Void> waitingPoll = onAnotherThread(p.timeoutRunner::pollOnce);
      p.awaitWaitingFor("1", 1);
      assertThat(p.runner.slots().inUse()).as("the step's slot and the claim's").isEqualTo(2);
      release.countDown();

      assertThat(outcomeOf(waitingPoll)).isNull();
      assertThat(outcomeOf(delivery)).isNull();
      assertThat(Duration.ofNanos(System.nanoTime() - start))
          .as("the claim gets the lock when the step ends, long before its bound of 20 s")
          .isLessThan(Duration.ofSeconds(10));
      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.storedOrder("1")).doesNotContain("Shipped");
      assertThat(p.runner.slots().inUse()).isZero();
    }
  }

  @Test
  void aRunnerBuiltWithAStepLock_actsOnOneSagaAtATime_onThePollingThread() {
    try (var p = new SagaStepLockFixture()) {
      var timeouts =
          SagaTimeoutRunner.<State>builder()
              .decider(p.saga)
              .sagaStore(p.sagaStore)
              .commandBus(p.bus)
              .sagaType(SagaStepLockFixture.TYPE)
              .clock(p.clock)
              .stepLock(p.lock)
              .build();
      open(p, "1", "2", "3");
      var releases = new CopyOnWriteArrayList<String>();
      var threads = ConcurrentHashMap.<Thread>newKeySet();
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Release release) {
              releases.add(release.orderId());
              threads.add(Thread.currentThread());
            }
          };
      p.clock.advance(TIMEOUT);

      timeouts.pollOnce();

      assertThat(releases).containsExactly("1", "2", "3");
      assertThat(threads).containsExactly(Thread.currentThread());
    }
  }

  @Test
  void theWaitsOfTwoSagasInSteps_overlap() {
    Duration bound = Duration.ofSeconds(2);
    try (var p = new SagaStepLockFixture(new SagaStepLock(), bound, false)) {
      var hung = new HungSteps(p, "1", "2");
      p.clock.advance(TIMEOUT);
      for (int poll = 0; poll < SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING; poll++) {
        p.timeoutRunner.pollOnce();
      }

      long start = System.nanoTime();
      p.timeoutRunner.pollOnce();
      Duration took = Duration.ofNanos(System.nanoTime() - start);

      assertThat(took).isGreaterThanOrEqualTo(bound);
      assertThat(took)
          .as("two waits of one bound each, side by side")
          .isLessThan(bound.multipliedBy(2));
      assertThat(p.timeoutRunner.claimsDeferredPastTheWait()).isEqualTo(2);
      hung.returnAtLast();
    }
  }

  @Test
  void aCompensationDeliveredOnTheClaimsThread_runsItsPageThereUnderTheClaimsSlot() {
    try (var p = new SagaStepLockFixture()) {
      parallelism(p, 2);
      var subscription = new InMemoryEventSubscription(p.eventStore, p.runner.asEventListener());
      subscription.start();
      try {
        p.append(new Opened("1"));
        p.clock.advance(Duration.ofSeconds(1));
        p.append(new Opened("2"));
        p.clock.advance(TIMEOUT.plusSeconds(1));
        p.append(new Opened("3"));
        p.append(new Opened("4"));
        // Two events of two other sagas wait, undelivered, for the next delivery: the one the
        // first compensation command's append starts on the claim's thread.
        subscription.dropNotifications(true);
        p.append(new Advanced("3"));
        p.append(new Advanced("4"));
        subscription.dropNotifications(false);
        var shipThreads = new CopyOnWriteArrayList<String>();
        p.bus.beforeEveryCommand =
            command -> {
              if (command instanceof Ship) {
                shipThreads.add(Thread.currentThread().getName());
              }
            };

        long start = System.nanoTime();
        p.timeoutRunner.pollOnce();
        Duration took = Duration.ofNanos(System.nanoTime() - start);

        assertThat(took)
            .as("no step of the nested page waits for a slot the claims hold")
            .isLessThan(Duration.ofSeconds(2));
        assertThat(compensated(p, "1", "2")).isTrue();
        assertThat(p.storedOrder("3")).containsExactly("Reserved", "Shipped");
        assertThat(p.storedOrder("4")).containsExactly("Reserved", "Shipped");
        assertThat(shipThreads)
            .hasSize(2)
            .allSatisfy(name -> assertThat(name).contains("-claim-").doesNotContain("-step-"));
        assertThat(p.runner.slots().inUse()).isZero();
      } finally {
        subscription.close();
      }
    }
  }

  /** Runs {@code body} and returns the INFO lines the timeout runner logged meanwhile. */
  private static List<String> timeoutInfoDuring(Runnable body) {
    var log =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(SagaTimeoutRunner.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    var before = log.getLevel();
    log.setLevel(ch.qos.logback.classic.Level.INFO);
    appender.start();
    log.addAppender(appender);
    try {
      body.run();
    } finally {
      log.detachAppender(appender);
      log.setLevel(before);
    }
    return appender.list.stream()
        .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.INFO)
        .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
        .toList();
  }

  /**
   * A saga whose step holds the runner's only slot and does not return: the first polls pass its
   * claim over without waiting for a slot; the poll that waits for it waits for a slot and for the
   * step lock within one claim wait, and claims without a slot when none frees.
   */
  @Test
  void aClaimToWaitFor_thatGetsNoSlotWithinItsWait_isMadeWithoutASlot_underneathTheStep() {
    Duration bound = Duration.ofSeconds(2);
    try (var p = new SagaStepLockFixture(new SagaStepLock(), bound)) {
      parallelism(p, 1);
      var hung = new HungSteps(p, "1");
      p.clock.advance(TIMEOUT);
      for (int poll = 0; poll < SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING; poll++) {
        long start = System.nanoTime();
        p.timeoutRunner.pollOnce();
        assertThat(Duration.ofNanos(System.nanoTime() - start))
            .as("a claim that asks once is passed over without waiting for the step's slot")
            .isLessThan(bound);
      }
      assertThat(p.timeoutRunner.deferralsOf(SagaStepLockFixture.sagaOf("1")))
          .isEqualTo(SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING);

      long start = System.nanoTime();
      List<String> info = timeoutInfoDuring(p.timeoutRunner::pollOnce);
      Duration took = Duration.ofNanos(System.nanoTime() - start);

      assertThat(took).isGreaterThanOrEqualTo(bound);
      assertThat(took)
          .as("the slot and the step lock share one claim wait")
          .isLessThan(bound.multipliedBy(2));
      assertThat(info).anySatisfy(line -> assertThat(line).contains("goes on without a slot"));
      assertThat(p.row("1").episodeVersion()).as("claimed underneath the step").isNotNull();
      assertThat(p.timeoutRunner.claimsDeferredPastTheWait()).isZero();
      assertThat(p.runner.slots().inUse()).as("only the step's slot").isEqualTo(1);
      hung.returnAtLast();
      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.storedOrder("1"))
          .as("the step's forward command is refused at its commit")
          .containsExactly("Released", "Refunded");
    }
  }

  @Test
  void withTheFenceOff_aClaimToWaitForThatGetsNoSlot_isDeferredPastTheWait() {
    Duration bound = Duration.ofSeconds(2);
    try (var p = new SagaStepLockFixture(new SagaStepLock(), bound, false)) {
      parallelism(p, 1);
      var hung = new HungSteps(p, "1");
      p.clock.advance(TIMEOUT);
      for (int poll = 0; poll < SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING; poll++) {
        p.timeoutRunner.pollOnce();
      }

      long start = System.nanoTime();
      p.timeoutRunner.pollOnce();
      Duration took = Duration.ofNanos(System.nanoTime() - start);

      assertThat(took).isGreaterThanOrEqualTo(bound).isLessThan(bound.multipliedBy(2));
      assertThat(p.timeoutRunner.claimsDeferredPastTheWait()).isEqualTo(1);
      assertThat(p.row("1").episodeVersion()).as("never claimed underneath a step").isNull();
      hung.returnAtLast();
    }
  }

  /**
   * The runner's only slot is held by a step that does not return. Another saga past its deadline
   * is not claimed without a slot: its claim waits one claim wait and goes to the next poll. The
   * hung saga's claim, past the threshold, waits one shared claim wait and is made underneath the
   * step without a slot. Once the step has ended, a later poll claims the other saga.
   */
  @Test
  void whileAHungStepHoldsTheOnlySlot_anotherSagasClaimGoesToTheNextPoll_andTheHungSagaIsClaimed() {
    Duration bound = Duration.ofSeconds(1);
    try (var p = new SagaStepLockFixture(new SagaStepLock(), bound)) {
      parallelism(p, 1);
      open(p, "2"); // before the step that holds the only slot
      var hung = new HungSteps(p, "1");
      p.clock.advance(TIMEOUT);
      for (int poll = 0; poll < SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING; poll++) {
        p.timeoutRunner.pollOnce();
      }
      assertThat(p.row("2").episodeVersion())
          .as("postponed, never claimed without a slot")
          .isNull();
      assertThat(p.timeoutRunner.deferralsOf(SagaStepLockFixture.sagaOf("2"))).isZero();

      long start = System.nanoTime();
      p.timeoutRunner.pollOnce();
      Duration took = Duration.ofNanos(System.nanoTime() - start);

      assertThat(took)
          .as("one claim wait for the other saga, then one shared wait for the hung saga")
          .isGreaterThanOrEqualTo(bound.multipliedBy(2))
          .isLessThan(bound.multipliedBy(4));
      assertThat(p.row("1").episodeVersion()).as("claimed underneath the hung step").isNotNull();
      assertThat(p.row("2").episodeVersion()).isNull();
      assertThat(p.timeoutRunner.claimsDeferredPastTheWait()).isZero();

      hung.returnAtLast();
      p.timeoutRunner.pollOnce();

      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.row("2").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.runner.slots().inUse()).isZero();
    }
  }

  /**
   * Counts the compensation commands dispatched and the most slots held whenever a command is
   * dispatched, from the moment it is installed.
   */
  private static final class Watch {
    final AtomicInteger compensations = new AtomicInteger();
    final AtomicInteger mostSlots = new AtomicInteger();

    Watch(SagaStepLockFixture p) {
      p.bus.beforeEveryCommand =
          command -> {
            mostSlots.accumulateAndGet(p.runner.slots().inUse(), Math::max);
            if (command instanceof Release || command instanceof Refund) {
              compensations.incrementAndGet();
            }
          };
    }
  }

  /**
   * A saga the poll waits for whose step ends before the waiting poll, while a step of another saga
   * holds the runner's only slot: its claim is not made without a slot, the polls that passed it
   * over are forgotten, and the next poll claims it on a slot like any other saga.
   */
  @Test
  void aClaimToWaitFor_whoseStepHasEnded_isNotMadeWithoutASlot_andTakesASlotOnTheNextPoll() {
    Duration bound = Duration.ofMillis(500);
    try (var p = new SagaStepLockFixture(new SagaStepLock(), bound)) {
      parallelism(p, 1);
      var hung = new HungSteps(p, "1");
      p.clock.advance(TIMEOUT);
      for (int poll = 0; poll < SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING; poll++) {
        p.timeoutRunner.pollOnce();
      }
      assertThat(p.timeoutRunner.deferralsOf(SagaStepLockFixture.sagaOf("1")))
          .isEqualTo(SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING);
      hung.returnAtLast(); // saga 1's step ends
      var other = new HungSteps(p, "2"); // a saga within its deadline; its step holds the slot
      var watch = new Watch(p);

      List<String> info = timeoutInfoDuring(p.timeoutRunner::pollOnce);

      assertThat(info).noneSatisfy(line -> assertThat(line).contains("without a slot"));
      assertThat(info).anySatisfy(line -> assertThat(line).contains("were postponed"));
      assertThat(p.row("1").episodeVersion()).as("not claimed without a slot").isNull();
      assertThat(watch.compensations.get()).as("steps and claims: the one step").isZero();
      assertThat(p.timeoutRunner.deferralsOf(SagaStepLockFixture.sagaOf("1"))).isZero();
      assertThat(p.timeoutRunner.claimsDeferredPastTheWait()).isZero();
      assertThat(p.runner.slots().inUse()).as("only the other step's slot").isEqualTo(1);

      other.returnAtLast();
      p.timeoutRunner.pollOnce();

      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(watch.compensations.get()).isEqualTo(2);
      assertThat(watch.mostSlots.get()).isLessThanOrEqualTo(1);
      assertThat(p.runner.slots().inUse()).isZero();
    }
  }

  /**
   * Saga 1 is a re-pick: claimed, its refund failing transiently. A correlated event resumes its
   * episode on the event path, and that resume step hangs across three polls, which pass the saga
   * over. Once the step has ended, the polls that passed it over are forgotten.
   */
  private static void aRePickPassedOverThreeTimes(SagaStepLockFixture p) {
    open(p, "1");
    p.refund.failure = () -> new IllegalStateException("connection reset");
    p.clock.advance(TIMEOUT);
    p.timeoutRunner.pollOnce();
    assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATING);
    p.clock.advance(TIMEOUT.plusSeconds(1)); // idle long enough to be re-picked
    var release = new CountDownLatch(1);
    var reached = new CountDownLatch(1);
    p.bus.beforeEveryCommand =
        command -> {
          if (command instanceof Release r && r.orderId().equals("1")) {
            reached.countDown();
            park(release);
          }
        };
    EventEnvelope advanced = p.append(new Advanced("1"));
    CompletableFuture<Void> resume = onAnotherThread(() -> p.deliver(advanced));
    await(reached);
    for (int poll = 0; poll < SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING; poll++) {
      p.timeoutRunner.pollOnce();
    }
    assertThat(p.timeoutRunner.deferralsOf(SagaStepLockFixture.sagaOf("1")))
        .isEqualTo(SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING);
    p.bus.beforeEveryCommand = null;
    release.countDown();
    outcomeOf(resume); // the resume's refund fails again: the episode stays a re-pick
    assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATING);
  }

  @Test
  void aRePickPassedOverWhileAResumeStepHeldItsLock_isResumedOnASlot_andItsCountIsForgotten() {
    try (var p = new SagaStepLockFixture(new SagaStepLock(), Duration.ofMillis(500))) {
      parallelism(p, 1);
      aRePickPassedOverThreeTimes(p);
      var watch = new Watch(p);

      p.timeoutRunner.pollOnce();

      assertThat(watch.compensations.get()).as("resumed: Release and Refund again").isEqualTo(2);
      assertThat(watch.mostSlots.get()).isEqualTo(1);
      assertThat(p.timeoutRunner.deferralsOf(SagaStepLockFixture.sagaOf("1"))).isZero();
    }
  }

  @Test
  void aRePickPassedOverWhileAResumeStepHeldItsLock_isNotResumedWithoutASlot_onceThatStepEnded() {
    try (var p = new SagaStepLockFixture(new SagaStepLock(), Duration.ofMillis(500))) {
      parallelism(p, 1);
      aRePickPassedOverThreeTimes(p);
      var other = new HungSteps(p, "2"); // a saga within its deadline; its step holds the slot
      var watch = new Watch(p);

      List<String> info = timeoutInfoDuring(p.timeoutRunner::pollOnce);

      assertThat(info).noneSatisfy(line -> assertThat(line).contains("without a slot"));
      assertThat(watch.compensations.get()).as("steps and claims: the one step").isZero();
      assertThat(p.timeoutRunner.deferralsOf(SagaStepLockFixture.sagaOf("1"))).isZero();

      other.returnAtLast();
      p.refund.failure = null;
      p.timeoutRunner.pollOnce();

      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(watch.mostSlots.get()).isLessThanOrEqualTo(1);
      assertThat(p.runner.slots().inUse()).isZero();
    }
  }

  /**
   * A step that does not return holds the runner's only slot, and eight other sagas are past their
   * deadline. Each poll waits one claim wait for the first of them and postpones the rest at once;
   * the hung saga is claimed underneath its step by the fourth poll, about five claim waits after
   * the first, not eight claim waits a poll later.
   */
  @Test
  void whileAHungStepHoldsTheOnlySlot_aPollCostsOneClaimWait_andTheHungSagaIsClaimedInTime() {
    Duration bound = Duration.ofSeconds(1);
    String[] others = {"2", "3", "4", "5", "6", "7", "8", "9"};
    try (var p = new SagaStepLockFixture(new SagaStepLock(), bound)) {
      parallelism(p, 1);
      open(p, others);
      var hung = new HungSteps(p, "1");
      p.clock.advance(TIMEOUT);

      long first = System.nanoTime();
      List<String> passing =
          timeoutInfoDuring(
              () -> {
                for (int poll = 0; poll < SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING; poll++) {
                  long start = System.nanoTime();
                  p.timeoutRunner.pollOnce();
                  assertThat(Duration.ofNanos(System.nanoTime() - start))
                      .as("one claim wait for the first saga, the other seven postponed at once")
                      .isGreaterThanOrEqualTo(bound)
                      .isLessThan(bound.multipliedBy(4));
                }
              });
      assertThat(passing)
          .as("logged on the first cycle of the streak")
          .filteredOn(line -> line.contains("were postponed"))
          .singleElement()
          .satisfies(line -> assertThat(line).startsWith("8 deadline claim(s)"));
      long start = System.nanoTime();
      List<String> info = timeoutInfoDuring(p.timeoutRunner::pollOnce);
      Duration took = Duration.ofNanos(System.nanoTime() - start);

      assertThat(took)
          .as("one claim wait for the batch, one for the hung saga")
          .isGreaterThanOrEqualTo(bound.multipliedBy(2))
          .isLessThan(bound.multipliedBy(5));
      assertThat(Duration.ofNanos(System.nanoTime() - first))
          .as("three polls of one claim wait, the fourth of two")
          .isLessThan(bound.multipliedBy(9));
      assertThat(p.row("1").episodeVersion()).as("claimed underneath the hung step").isNotNull();
      assertThat(info).anySatisfy(line -> assertThat(line).contains("goes on without a slot"));
      for (String order : others) {
        assertThat(p.row(order).episodeVersion()).isNull();
      }

      hung.returnAtLast();
      p.timeoutRunner.pollOnce();

      assertThat(compensated(p, "1")).isTrue();
      assertThat(compensated(p, others)).isTrue();
      assertThat(p.runner.slots().inUse()).isZero();
    }
  }

  /**
   * {@code close()} while three claim workers are in flight: two inside a compensation command, one
   * waiting for the step lock of a saga whose step does not return. Each is interrupted, the close
   * returns long before the claim wait, nothing is left holding a slot, and the runner starts
   * again.
   */
  @Test
  void aCloseWithClaimWorkersInFlight_interruptsEachOfThem_andTheRunnerStartsAgain() {
    Duration claimWait = Duration.ofSeconds(20);
    try (var p = new SagaStepLockFixture(new SagaStepLock(), claimWait)) {
      parallelism(p, 4);
      var timeouts =
          SagaTimeoutRunner.<State>builder()
              .decider(p.saga)
              .sagaStore(p.sagaStore)
              .commandBus(p.bus)
              .sagaType(SagaStepLockFixture.TYPE)
              .clock(p.clock)
              .sagaRunner(p.runner)
              .claimWait(claimWait)
              .pollInterval(Duration.ofHours(1))
              .build();
      var ending = new HungSteps(p, "1", "2");
      var hung = new HungSteps(p, "3");
      p.clock.advance(TIMEOUT);
      for (int poll = 0; poll < SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING; poll++) {
        timeouts.pollOnce();
      }
      ending.returnAtLast(); // the steps of sagas 1 and 2 end; saga 3's does not
      var inCompensation = new CountDownLatch(2);
      var interrupted = new ConcurrentHashMap<String, Boolean>();
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Release release) {
              inCompensation.countDown();
              long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
              while (!Thread.currentThread().isInterrupted() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
              }
              interrupted.put(release.orderId(), Thread.currentThread().isInterrupted());
              // A command that does not stop on an interrupt: it completes, and so does the saga.
              Thread.interrupted();
            }
          };

      timeouts.start();
      await(inCompensation);
      p.awaitWaitingFor("3", 1);
      long start = System.nanoTime();
      timeouts.close();
      Duration took = Duration.ofNanos(System.nanoTime() - start);

      assertThat(took).as("no worker waited out its claim wait").isLessThan(Duration.ofSeconds(10));
      awaitUntil("both compensations saw their interrupt", () -> interrupted.size() == 2);
      assertThat(interrupted).containsEntry("1", true).containsEntry("2", true);
      assertThat(p.lock.waitingFor(SagaStepLockFixture.TYPE, SagaStepLockFixture.sagaOf("3")))
          .as("the claim waiting for the step lock was interrupted")
          .isZero();
      assertThat(p.row("3").episodeVersion()).isNull();
      assertThat(timeouts.isAlive()).isFalse();
      assertThat(timeouts.isStarted()).isFalse();
      assertThat(p.runner.slots().inUse()).as("only the hung step's slot").isEqualTo(1);

      hung.returnAtLast();
      assertThat(p.runner.slots().inUse()).isZero();
      p.bus.beforeEveryCommand = null;
      p.clock.advance(TIMEOUT.plusSeconds(1)); // the claimed episodes are re-picked
      timeouts.start();
      try {
        awaitUntil(
            "the restarted runner compensates all three", () -> compensated(p, "1", "2", "3"));
      } finally {
        timeouts.close();
      }
      assertThat(p.runner.slots().inUse()).isZero();
    }
  }
}
