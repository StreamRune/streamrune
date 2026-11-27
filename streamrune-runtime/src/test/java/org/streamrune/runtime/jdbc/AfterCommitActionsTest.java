package org.streamrune.runtime.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.types.ProjectionName;

class AfterCommitActionsTest {

  private static final ProjectionName P = ProjectionName.of("p");

  /** The category a repository would pass: its own. */
  private static final String STORE_CATEGORY = "org.streamrune.runtime.jdbc.TestStore";

  private static final String WARN =
      "Projection 'p' after-commit action failed; the batch stays committed and the remaining"
          + " after-commit actions still run";

  private Capture store;
  private Capture helper;
  private final List<String> ran = new ArrayList<>();

  @BeforeEach
  void attach() {
    store = Capture.attach(STORE_CATEGORY);
    helper = Capture.attach(AfterCommitActions.class.getName());
  }

  @AfterEach
  void detach() {
    store.close();
    helper.close();
  }

  private AfterCommitActions actions() {
    return new AfterCommitActions(P, LoggerFactory.getLogger(STORE_CATEGORY));
  }

  private Runnable record(String name) {
    return () -> ran.add(name);
  }

  @Test
  void actionsRunOnlyAfterCommitIssued() {
    AfterCommitActions actions = actions();
    actions.add(record("a"));

    // No COMMIT issued: a rollback discards them, and later actions still queue.
    actions.runIfCommitIssued(null);
    actions.runIfCommitIssued(new RuntimeException("rolled back"));
    actions.add(record("b"));
    assertThat(ran).isEmpty();

    actions.commitIssued();
    assertThat(ran).isEmpty();
    actions.runIfCommitIssued(null);
    assertThat(ran).containsExactly("a", "b");

    // Run once: a second run finds nothing queued.
    actions.runIfCommitIssued(null);
    assertThat(ran).containsExactly("a", "b");

    // A transaction that fails before its COMMIT: the actions are discarded, its failure thrown.
    var failure = new IllegalStateException("fenced");
    Throwable thrown =
        catchThrowable(
            () ->
                AfterCommitActions.runCheckpointTransaction(
                    P,
                    LoggerFactory.getLogger(STORE_CATEGORY),
                    a -> {
                      a.add(record("discarded"));
                      throw failure;
                    }));
    assertThat(thrown).isSameAs(failure);
    assertThat(thrown.getSuppressed()).isEmpty();
    assertThat(ran).containsExactly("a", "b");
  }

  @Test
  void aFailingActionWarnsOnTheGivenLoggerAndTheRestRun() {
    AfterCommitActions actions = actions();
    var boom = new IllegalStateException("evict failed");
    actions.add(record("a"));
    actions.add(
        () -> {
          throw boom;
        });
    actions.add(record("c"));
    actions.commitIssued();

    actions.runIfCommitIssued(null);

    assertThat(ran).containsExactly("a", "c");
    List<ILoggingEvent> warns = store.events(Level.WARN);
    assertThat(warns).hasSize(1);
    assertThat(warns.get(0).getFormattedMessage()).isEqualTo(WARN);
    assertThat(warns.get(0).getThrowableProxy().getClassName())
        .isEqualTo(IllegalStateException.class.getName());
    assertThat(warns.get(0).getThrowableProxy().getMessage()).isEqualTo("evict failed");
    assertThat(store.all()).hasSize(1);
    assertThat(helper.all()).isEmpty();
  }

  @Test
  void anErrorIsSuppressedOnTheInFlightFailure() {
    AfterCommitActions actions = actions();
    var error = new AssertionError("eviction error");
    actions.add(record("a"));
    actions.add(
        () -> {
          throw error;
        });
    actions.add(record("after the error"));
    actions.commitIssued();
    var inFlight = new RuntimeException("commit failed");

    actions.runIfCommitIssued(inFlight);

    assertThat(ran).containsExactly("a");
    assertThat(inFlight.getSuppressed()).containsExactly(error);
    assertThat(store.all()).isEmpty();

    // The same Error instance in flight (a preallocated JVM Error) is not suppressed on itself.
    AfterCommitActions again = actions();
    again.add(
        () -> {
          throw error;
        });
    again.commitIssued();
    again.runIfCommitIssued(error);
    assertThat(error.getSuppressed()).isEmpty();
  }

  @Test
  void anErrorWithoutAnInFlightFailureIsThrown() {
    AfterCommitActions actions = actions();
    var error = new AssertionError("eviction error");
    actions.add(
        () -> {
          throw error;
        });
    actions.add(record("after the error"));
    actions.commitIssued();

    assertThat(catchThrowable(() -> actions.runIfCommitIssued(null))).isSameAs(error);
    assertThat(ran).isEmpty();
    assertThat(store.all()).isEmpty();
  }

  @Test
  void actionsAddedAfterTheRunRunAtOnce() {
    AfterCommitActions actions = actions();
    // An action registering another while the actions run: the second runs at once.
    actions.add(
        () -> {
          ran.add("outer");
          actions.add(record("nested"));
          ran.add("outer done");
        });
    actions.commitIssued();
    actions.runIfCommitIssued(null);
    assertThat(ran).containsExactly("outer", "nested", "outer done");

    // Through a repository kept past the transaction: at once, and a failure is only logged.
    actions.add(record("late"));
    assertThat(ran).containsExactly("outer", "nested", "outer done", "late");
    actions.add(
        () -> {
          throw new IllegalStateException("late failure");
        });
    assertThat(store.events(Level.WARN))
        .extracting(ILoggingEvent::getFormattedMessage)
        .containsExactly(WARN);
  }

  @Test
  void runCheckpointTransaction_runsActionsAlsoWhenALaterStepThrew() {
    // A clean transaction: the actions run once it returned.
    AfterCommitActions.runCheckpointTransaction(
        P,
        LoggerFactory.getLogger(STORE_CATEGORY),
        a -> {
          a.add(record("clean"));
          a.commitIssued();
          ran.add("committed");
        });
    assertThat(ran).containsExactly("committed", "clean");

    // The COMMIT (or the close after it) threw: the actions run, and that failure is thrown.
    ran.clear();
    var commitFailure = new IllegalStateException("commit failed");
    Throwable thrown =
        catchThrowable(
            () ->
                AfterCommitActions.runCheckpointTransaction(
                    P,
                    LoggerFactory.getLogger(STORE_CATEGORY),
                    a -> {
                      a.add(record("after an unknown outcome"));
                      a.commitIssued();
                      throw commitFailure;
                    }));
    assertThat(thrown).isSameAs(commitFailure);
    assertThat(ran).containsExactly("after an unknown outcome");

    // An Error from an action rides on the failure in flight, which stays the one thrown.
    var error = new AssertionError("eviction error");
    var closeFailure = new IllegalStateException("close failed");
    thrown =
        catchThrowable(
            () ->
                AfterCommitActions.runCheckpointTransaction(
                    P,
                    LoggerFactory.getLogger(STORE_CATEGORY),
                    a -> {
                      a.add(
                          () -> {
                            throw error;
                          });
                      a.commitIssued();
                      throw closeFailure;
                    }));
    assertThat(thrown).isSameAs(closeFailure);
    assertThat(closeFailure.getSuppressed()).containsExactly(error);

    // After a clean commit the Error propagates.
    thrown =
        catchThrowable(
            () ->
                AfterCommitActions.runCheckpointTransaction(
                    P,
                    LoggerFactory.getLogger(STORE_CATEGORY),
                    a -> {
                      a.add(
                          () -> {
                            throw error;
                          });
                      a.commitIssued();
                    }));
    assertThat(thrown).isSameAs(error);
    assertThat(helper.all()).isEmpty();
  }

  /**
   * The events of one logger category: a logback {@link ListAppender} on the logger of that name,
   * at TRACE and not additive while attached.
   */
  private static final class Capture implements AutoCloseable {
    private final Logger logger;
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private final Level previousLevel;
    private final boolean previousAdditivity;

    private Capture(Logger logger) {
      this.logger = logger;
      this.previousLevel = logger.getLevel();
      this.previousAdditivity = logger.isAdditive();
      appender.start();
      logger.addAppender(appender);
      logger.setLevel(Level.TRACE);
      logger.setAdditive(false);
    }

    static Capture attach(String name) {
      return new Capture((Logger) LoggerFactory.getLogger(name));
    }

    List<ILoggingEvent> events(Level level) {
      return all().stream().filter(e -> e.getLevel() == level).toList();
    }

    List<ILoggingEvent> all() {
      synchronized (appender) {
        return List.copyOf(appender.list);
      }
    }

    @Override
    public void close() {
      logger.setLevel(previousLevel);
      logger.setAdditive(previousAdditivity);
      logger.detachAppender(appender);
      appender.stop();
    }
  }
}
