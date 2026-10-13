package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.ForwardingSagaStore.SimulatedCrash;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Advanced;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Cancelled;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Harness;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Nudged;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Opened;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Refunded;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Released;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Reserve;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Reserved;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Ship;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.Shipped;
import org.streamrune.runtime.SagaEpisodeStampReadFromRowTest.State;

/**
 * A start event whose own {@code evolve} yields {@code COMPENSATING} runs its forward commands
 * before its row can be resumed as a compensation episode. The row is inserted genesis-pending with
 * the forward status {@code STARTED}, holding the evolved state; the event write after the forward
 * loop lands {@code COMPENSATING} and stamps the episode with the start event as its cause — what
 * the write after a correlated event's forward loop does.
 *
 * <p>Two things follow, and this class pins both. A retry-later failure or a crash inside the
 * forward loop leaves an ordinary genesis-pending row, so the redelivery re-evolves, dispatches the
 * forward commands again under their forward keys and only then enters the episode: no forward
 * command is dropped. And no resume path sees a compensation episode while the forward loop runs: a
 * second delivery of the start event re-drives the forward loop, a later correlated event is held
 * as genesis-pending, the sweeper does not see the row; the stored events have every forward effect
 * before its compensation.
 *
 * <p>Uses the harness of {@link SagaEpisodeStampReadFromRowTest}. No thread, no clock wait: a
 * second path runs in a hook at a fixed point, and a process death is an {@link Error} thrown at a
 * fixed point of the delivery.
 */
class SagaStartForwardBeforeCompensatingTest {

  private static final String ORDER = "42";
  private static final SagaId SAGA = SagaId.of("order-saga-" + ORDER);
  private static final SagaType TYPE = SagaType.fromClass(State.class);
  private static final Duration TIMEOUT = Duration.ofMinutes(5);
  private static final Duration RETRY_INTERVAL = Duration.ofSeconds(30);

  /**
   * The process dies at this point: nothing after it runs, no catch block of the runtime sees it.
   */
  static final class ProcessDeath extends Error {
    ProcessDeath() {
      super("the process dies here", null, false, false);
    }
  }

  private static Harness bornCompensating() {
    return new Harness(true, 0);
  }

  /** The stored forward and compensation events of the saga, by name, in global order. */
  private static List<String> stored(Harness h) {
    return h.eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
        .map(e -> (DomainEvent) e.event())
        .filter(
            e ->
                e instanceof Reserved
                    || e instanceof Shipped
                    || e instanceof Released
                    || e instanceof Refunded)
        .map(e -> e.getClass().getSimpleName())
        .toList();
  }

  private static void sweep(Harness h) {
    h.clock.advance(RETRY_INTERVAL.plusSeconds(1));
    h.sweeper.sweepOnce();
  }

  /** The row while the forward loop of the start event runs. */
  private static void assertGenesisPendingForwardRow(LoadedSaga<State> row) {
    assertThat(row.status())
        .as("a forward status until the forward loop ends")
        .isEqualTo(SagaStatus.STARTED);
    assertThat(row.state().status())
        .as("the evolved state is persisted")
        .isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.genesisApplied()).isFalse();
    assertThat(row.version()).isEqualTo(1L);
    assertThat(row.episodeVersion()).as("no episode yet").isNull();
    assertThat(row.episodeClaimedAt()).isNull();
    assertThat(row.episodeCauseEventId()).isNull();
  }

  /** The row once the event write after the forward loop has landed. */
  private static void assertEpisodeEnteredByTheStartEvent(
      LoadedSaga<State> row, EventEnvelope start) {
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.genesisApplied()).isTrue();
    assertThat(row.episodeVersion())
        .as("stamped by the write that landed COMPENSATING")
        .isEqualTo(row.version());
    assertThat(row.episodeClaimedAt()).isNotNull();
    assertThat(row.episodeCauseEventId()).isEqualTo(start.metadata().eventId());
  }

  @Test
  void theRowIsInsertedWithAForwardStatus_andTheWriteAfterTheForwardLoopEntersTheEpisode() {
    try (var h = bornCompensating()) {
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.bus.runOnceBefore(Reserve.class, () -> assertGenesisPendingForwardRow(h.row()));

      h.deliver(opened);

      assertEpisodeEnteredByTheStartEvent(h.row(), opened);
      assertThat(h.row().version()).isEqualTo(2L);
      assertThat(stored(h)).containsExactly("Reserved");

      sweep(h);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(stored(h)).containsExactly("Reserved", "Released", "Refunded");
      assertThat(h.compensationCauses()).containsExactly(h.causeOfTheRowsEpisode());
    }
  }

  /** A start event that evolves to an ordinary forward status is inserted with that status. */
  @Test
  void aStartEventThatEvolvesToAForwardStatus_isInsertedWithIt() {
    try (var h = new Harness(false, 0)) {
      h.bus.runOnceBefore(
          Reserve.class,
          () -> {
            assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
            assertThat(h.row().genesisApplied()).isFalse();
            assertThat(h.row().version()).isEqualTo(1L);
          });

      h.deliver(h.append(new Opened(ORDER)));

      assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
      assertThat(h.row().genesisApplied()).isTrue();
      assertThat(h.row().episodeVersion()).isNull();
      assertThat(stored(h)).containsExactly("Reserved");
    }
  }

  /** A correlated event that evolves to COMPENSATING enters the episode after its forward loop. */
  @Test
  void aCorrelatedEventThatEvolvesToCompensating_entersTheEpisodeAfterItsForwardLoop() {
    try (var h = new Harness(false, 0)) {
      h.deliver(h.append(new Opened(ORDER)));
      EventEnvelope cancelled = h.append(new Cancelled(ORDER));
      h.bus.runOnceBefore(
          Ship.class,
          () -> {
            assertThat(h.row().status()).isEqualTo(SagaStatus.RUNNING);
            assertThat(h.row().episodeVersion()).isNull();
          });

      h.deliver(cancelled);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(h.row().episodeVersion()).isEqualTo(h.row().version());
      assertThat(h.row().episodeCauseEventId()).isEqualTo(cancelled.metadata().eventId());
      assertThat(stored(h)).containsExactly("Reserved", "Shipped");
    }
  }

  /**
   * The forward command fails with retry-later evidence. The row stays the genesis-pending forward
   * row, the failure propagates, and the redelivery dispatches the command again under the same
   * forward key before it enters the episode.
   */
  @Test
  void aRetryLaterFailureOfTheForwardCommand_theRedeliveryDispatchesItAgain() {
    try (var h = bornCompensating()) {
      h.reserve.failure = () -> new IllegalStateException(new SQLException("connection reset"));
      EventEnvelope opened = h.append(new Opened(ORDER));

      assertThatThrownBy(() -> h.deliver(opened)).isInstanceOf(IllegalStateException.class);

      assertGenesisPendingForwardRow(h.row());
      assertThat(stored(h)).isEmpty();
      List<String> firstAttempt = List.copyOf(h.bus.keys);
      h.reserve.failure = null;
      h.bus.keys.clear();

      h.deliver(opened);

      assertThat(h.bus.keys).as("the same forward key").isEqualTo(firstAttempt);
      assertThat(stored(h)).containsExactly("Reserved");
      assertEpisodeEnteredByTheStartEvent(h.row(), opened);

      sweep(h);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(stored(h)).containsExactly("Reserved", "Released", "Refunded");
    }
  }

  /**
   * The process dies between the two forward commands of the start event. The redelivery re-evolves
   * from the initial state, the first command answers from the inbox, the second is dispatched, and
   * the episode is entered after it.
   */
  @Test
  void theProcessDiesInTheForwardLoop_theRedeliveryCompletesTheLoop() {
    try (var h = bornCompensating()) {
      h.saga.startAlsoShips = true;
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.bus.runOnceBefore(
          Ship.class,
          () -> {
            throw new ProcessDeath();
          });

      assertThatThrownBy(() -> h.deliver(opened)).isInstanceOf(ProcessDeath.class);

      assertGenesisPendingForwardRow(h.row());
      assertThat(stored(h)).containsExactly("Reserved");

      h.deliver(opened);

      assertThat(stored(h)).containsExactly("Reserved", "Shipped");
      assertEpisodeEnteredByTheStartEvent(h.row(), opened);

      sweep(h);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(stored(h)).containsExactly("Reserved", "Shipped", "Released", "Refunded");
    }
  }

  /**
   * A second path arrives while the opening delivery is about to dispatch its forward command. None
   * of them finds a compensation episode: the forward effect is stored before any compensation, and
   * the saga ends COMPENSATED with that effect undone.
   */
  @TestFactory
  Stream<DynamicTest> aSecondPathDuringTheForwardLoop_findsNoEpisode() {
    EventEnvelope[] opened = new EventEnvelope[1];
    Map<String, Consumer<Harness>> paths = new LinkedHashMap<>();
    paths.put(
        "aSecondDeliveryOfTheStartEvent",
        h -> h.secondReplica.asEventListener().onEvents(List.of(opened[0])));
    paths.put(
        "aLaterCorrelatedEventOnAnotherReplica",
        h -> h.secondReplica.asEventListener().onEvents(List.of(h.append(new Nudged(ORDER)))));
    paths.put("theSweeper", SagaStartForwardBeforeCompensatingTest::sweep);
    return paths.entrySet().stream()
        .map(
            path ->
                DynamicTest.dynamicTest(
                    path.getKey(),
                    () -> {
                      try (var h = bornCompensating()) {
                        opened[0] = h.append(new Opened(ORDER));
                        h.bus.runOnceBefore(
                            Reserve.class,
                            () -> {
                              path.getValue().accept(h);
                              assertThat(stored(h))
                                  .as("the second path dispatched no compensation")
                                  .doesNotContain("Released", "Refunded");
                            });

                        try {
                          h.deliver(opened[0]);
                        } catch (org.streamrune.core.OptimisticLockException lostTheEventWrite) {
                          // The second delivery landed the event write first: the redelivery
                          // routes on the row it finds.
                          h.deliver(opened[0]);
                        }
                        if (h.row().status() == SagaStatus.COMPENSATING) {
                          sweep(h);
                        }

                        assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
                        assertThat(stored(h)).containsExactly("Reserved", "Released", "Refunded");
                        assertThat(h.row().episodeCauseEventId())
                            .isEqualTo(opened[0].metadata().eventId());
                      }
                    }));
  }

  /** A correlated event that arrives during the forward loop is held, not applied and not lost. */
  @Test
  void aCorrelatedEventDuringTheForwardLoop_isHeldAsGenesisPending_andDrainedAfterwards() {
    try (var h = bornCompensating()) {
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.bus.runOnceBefore(
          Reserve.class,
          () -> {
            h.secondReplica.asEventListener().onEvents(List.of(h.append(new Nudged(ORDER))));
            assertThat(h.deadLetters.findBySaga(SAGA)).as("held behind the genesis").hasSize(1);
            assertGenesisPendingForwardRow(h.row());
          });

      h.deliver(opened);

      assertEpisodeEnteredByTheStartEvent(h.row(), opened);

      h.replayer.replayAll(SAGA);

      assertThat(h.deadLetters.findBySaga(SAGA)).isEmpty();
      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(stored(h)).containsExactly("Reserved", "Released", "Refunded");
    }
  }

  /** Where the opening delivery of a two-command start stops. */
  private static Map<String, Consumer<Harness>> crashPoints() {
    Map<String, Consumer<Harness>> points = new LinkedHashMap<>();
    points.put(
        "afterTheInsert",
        h ->
            h.bus.runOnceBefore(
                Reserve.class,
                () -> {
                  throw new ProcessDeath();
                }));
    points.put(
        "afterTheFirstForwardCommand",
        h ->
            h.bus.runOnceBefore(
                Ship.class,
                () -> {
                  throw new ProcessDeath();
                }));
    points.put(
        "afterTheForwardLoop",
        h ->
            h.sagaStore.runOnceBeforeTheEventWrite(
                () -> {
                  throw new ProcessDeath();
                }));
    points.put("afterTheEventWrite", h -> h.sagaStore.crashOnceAfter("applyEvent"));
    return points;
  }

  /**
   * Every crash point of the opening delivery of a two-command start born COMPENSATING, followed by
   * each path that can come next, and then by the redelivery of the start event and a sweep. In
   * every combination no forward effect is stored after a compensation, nothing is stored twice,
   * and the saga ends COMPENSATED. A path that is not the timeout runner never compensates before
   * the forward loop has finished, so both forward effects are stored; the timeout runner claims a
   * start that died past its deadline and compensates what the persisted state names, and the
   * redelivery then finds a terminal row.
   */
  @TestFactory
  Stream<DynamicTest> everyCrashPointOfTheOpeningDelivery_followedByEachPath() {
    EventEnvelope[] opened = new EventEnvelope[1];
    Map<String, Consumer<Harness>> followers = new LinkedHashMap<>();
    followers.put("theRedelivery", h -> h.deliver(opened[0]));
    followers.put("theSweeper", SagaStartForwardBeforeCompensatingTest::sweep);
    followers.put("aLaterCorrelatedEvent", h -> h.deliver(h.append(new Nudged(ORDER))));
    followers.put(
        "theTimeoutRunner",
        h -> {
          h.clock.advance(TIMEOUT.plusSeconds(1));
          h.timeoutRunner.pollOnce();
        });
    return crashPoints().entrySet().stream()
        .flatMap(
            point ->
                followers.entrySet().stream()
                    .map(
                        follower ->
                            DynamicTest.dynamicTest(
                                point.getKey() + "_then_" + follower.getKey(),
                                () -> {
                                  try (var h = bornCompensating()) {
                                    h.saga.startAlsoShips = true;
                                    opened[0] = h.append(new Opened(ORDER));
                                    point.getValue().accept(h);
                                    assertThatThrownBy(() -> h.deliver(opened[0]))
                                        .isInstanceOfAny(ProcessDeath.class, SimulatedCrash.class);
                                    boolean episodeEntered =
                                        point.getKey().equals("afterTheEventWrite");
                                    if (episodeEntered) {
                                      assertEpisodeEnteredByTheStartEvent(h.row(), opened[0]);
                                    } else {
                                      assertGenesisPendingForwardRow(h.row());
                                    }

                                    follower.getValue().accept(h);
                                    boolean timedOutBeforeTheLoopEnded =
                                        follower.getKey().equals("theTimeoutRunner")
                                            && !episodeEntered;
                                    if (!timedOutBeforeTheLoopEnded && !episodeEntered) {
                                      assertThat(stored(h))
                                          .as("no compensation before the forward loop ends")
                                          .doesNotContain("Released", "Refunded");
                                    }
                                    h.deliver(opened[0]);
                                    h.replayer.replayAll(SAGA);
                                    if (h.row().status() == SagaStatus.COMPENSATING) {
                                      sweep(h);
                                    }

                                    assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
                                    List<String> events = stored(h);
                                    assertThat(events).doesNotHaveDuplicates();
                                    assertThat(events)
                                        .as("every forward effect precedes its compensation")
                                        .endsWith("Released", "Refunded");
                                    if (timedOutBeforeTheLoopEnded) {
                                      assertThat(h.row().episodeCauseEventId())
                                          .as("the timeout opened the episode")
                                          .isNull();
                                    } else {
                                      assertThat(events)
                                          .containsExactly(
                                              "Reserved", "Shipped", "Released", "Refunded");
                                      assertThat(h.row().episodeCauseEventId())
                                          .isEqualTo(opened[0].metadata().eventId());
                                    }
                                  }
                                })));
  }

  /**
   * The forward command of the born start is rejected: the claim is the write that stamps the
   * episode, with the start event as its cause, and the opening call and a resume present the keys
   * of that stamp.
   */
  @Test
  void theForwardCommandIsRejected_theClaimStampsTheEpisode() {
    try (var h = bornCompensating()) {
      h.reserve.failure = () -> new IllegalArgumentException("out of stock");
      h.refund.failure = () -> new IllegalStateException("connection reset");
      EventEnvelope opened = h.append(new Opened(ORDER));

      h.deliver(opened);
      h.refund.failure = null;

      LoadedSaga<State> row = h.row();
      assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(row.version()).isEqualTo(2L);
      assertThat(row.episodeVersion()).as("the claim is the stamping write").isEqualTo(2L);
      assertThat(row.episodeCauseEventId()).isEqualTo(opened.metadata().eventId());
      assertThat(row.genesisApplied()).as("a claim never applies the genesis").isFalse();
      assertThat(h.bus.compensationKeys()).isEqualTo(h.keysOfTheRowsEpisode());

      h.bus.keys.clear();
      h.deliver(opened);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.bus.compensationKeys()).isEqualTo(h.keysOfTheRowsEpisode());
      h.assertEachCompensationStoredOnce();
      assertThat(stored(h)).containsExactly("Released", "Refunded");
    }
  }

  /**
   * {@code handle} throws on the born start. The fault is a genesis-pending forward fault: nothing
   * to resume. The replayed start entry re-evolves, dispatches the forward command and enters the
   * episode.
   */
  @Test
  void handleThrowsOnTheBornStart_theReplayedStartEntryRunsTheForwardLoop() {
    try (var h = bornCompensating()) {
      h.saga.poisonHandleOnStart = true;
      EventEnvelope opened = h.append(new Opened(ORDER));

      h.deliver(opened);

      assertThat(h.row().status()).isEqualTo(SagaStatus.FAULTED);
      assertThat(h.row().preFaultStatus()).as("a forward fault of a genesis-pending row").isNull();
      assertThat(h.row().episodeVersion()).isNull();
      assertThat(h.deadLetters.findBySaga(SAGA)).hasSize(1);
      h.saga.poisonHandleOnStart = false;

      h.replayer.replayAll(SAGA);

      assertThat(h.deadLetters.findBySaga(SAGA)).isEmpty();
      assertThat(stored(h)).containsExactly("Reserved");
      assertEpisodeEnteredByTheStartEvent(h.row(), opened);
    }
  }

  /**
   * A row that a store was asked to insert at COMPENSATING carries a stamp from birth (tooling, or
   * a row written before the start path inserted a forward status). A delivery of the start event
   * resumes that episode under the row's own stamp and cause.
   */
  @Test
  void aRowStampedAtBirth_isResumedUnderItsOwnStamp() {
    try (var h = bornCompensating()) {
      EventEnvelope opened = h.append(new Opened(ORDER));
      h.rows.createGenesisPending(
          SAGA,
          TYPE,
          new State(SagaStatus.COMPENSATING, ORDER),
          SagaStatus.COMPENSATING,
          false,
          EventId.of("evt-of-the-insert"));
      assertThat(h.row().episodeVersion()).isEqualTo(1L);

      h.deliver(opened);

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(h.bus.keys)
          .containsExactly(
              "Release saga:order-saga-42:episode:1:comp:0",
              "Refund saga:order-saga-42:episode:1:comp:1");
      assertThat(h.compensationCauses()).containsExactly(h.causeOfTheRowsEpisode());
      assertThat(h.row().episodeCauseEventId()).isEqualTo(EventId.of("evt-of-the-insert"));
    }
  }

  /** A second path at the correlated step is not this class's subject; the saga is unchanged. */
  @Test
  void anOrdinarySagaIsUnchanged_forwardRejectedOnACorrelatedEvent() {
    try (var h = new Harness(false, 0)) {
      h.deliver(h.append(new Opened(ORDER)));
      h.ship.failure = () -> new IllegalArgumentException("address rejected");

      h.deliver(h.append(new Advanced(ORDER)));

      assertThat(h.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(stored(h)).containsExactly("Reserved", "Released", "Refunded");
      assertThat(h.bus.compensationKeys()).isEqualTo(h.keysOfTheRowsEpisode());
    }
  }
}
