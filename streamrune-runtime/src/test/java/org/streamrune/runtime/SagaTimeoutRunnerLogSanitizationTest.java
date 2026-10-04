package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * {@link SagaTimeoutRunner} interpolated a stored {@code sagaId} straight into a {@code WARNING}. A
 * {@code SagaId} is derived from business identifiers carried by correlated events, so its bytes
 * can be chosen by whoever supplies those identifiers, and {@code SagaId} validates non-blank and a
 * 255-character ceiling only — every control character passes.
 *
 * <p>This is the sink that proves why the fix belongs at the sink and not at the source. The id is
 * read out of {@code saga_state} and interpolated into this WARNING <em>before</em> any {@code
 * RequestContext} is built, so the ingress bound ({@code IdConstraints}, applied at {@code
 * RequestContext#fromRequest}) never sees it. An earlier attempt to bound it on the reconstruction
 * path instead made this strictly worse: it turned one legacy row into a forged WARNING emitted on
 * every poll cycle.
 *
 * <p>{@code SagaTimeoutRunner} logs through SLF4J, which this module's tests bind to Logback. The
 * assertions therefore run against a real Logback appender and render each captured event with a
 * Logback {@code PatternLayout} — the actual bytes an operator or a log shipper would see — rather
 * than against the argument list.
 */
class SagaTimeoutRunnerLogSanitizationTest {

  private static final AggregateType TYPE = AggregateType.of("stream");

  /**
   * The exact CWE-117 shape: a newline followed by text formatted like a genuine log record, so a
   * reader (or a downstream SIEM's line-oriented parser) cannot tell it from real framework output.
   */
  private static final String FORGED_TAIL =
      "\nWARN SagaTimeoutRunner: Saga order-victim compensated successfully; no operator action"
          + " required";

  record OrderState(SagaId sagaId, SagaStatus status) implements SagaState {
    @JsonCreator
    OrderState(@JsonProperty("sagaId") SagaId sagaId, @JsonProperty("status") SagaStatus status) {
      this.sagaId = sagaId;
      this.status = status;
    }
  }

  record CancelOrder(String orderId) implements Command {}

  static class TimeoutDecider implements SagaDecider<OrderState> {
    @Override
    public Class<OrderState> stateType() {
      return OrderState.class;
    }

    @Override
    public OrderState initialState(SagaId sagaId) {
      return new OrderState(sagaId, SagaStatus.STARTED);
    }

    @Override
    public OrderState evolve(OrderState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(OrderState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        OrderState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofMinutes(5));
    }
  }

  static class NoopCommandBus implements CommandBus {
    /** The saga builder refuses to construct a runner over a bus without idempotent execution. */
    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("s")), new Version(1), List.of(), List.of());
    }

    @Override
    public <C extends Command> CommandResult execute(
        C command, org.streamrune.core.types.IdempotencyKey key) {
      return execute(command);
    }
  }

  /**
   * Captures {@code WARN}+ events of the {@link SagaTimeoutRunner} logger for the duration of
   * {@code body} and returns each one rendered by a Logback {@code PatternLayout} — i.e. the text a
   * file or console appender would actually write.
   *
   * <p>The attached {@link Throwable} is deliberately dropped before rendering ({@code %nopex}). A
   * stack trace is legitimately multi-line, and leaving it in would drown the property under test —
   * whether the {@code sagaId} can open a line of its own. The exception itself is untouched in
   * production; only this rendering omits it.
   */
  private static List<String> renderedWarningsDuring(Runnable body) {
    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(SagaTimeoutRunner.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      body.run();
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
    var layout = new ch.qos.logback.classic.PatternLayout();
    layout.setContext(logger.getLoggerContext());
    layout.setPattern("%level %logger{0}: %msg%n%nopex");
    layout.start();
    return appender.list.stream()
        .filter(e -> e.getLevel().isGreaterOrEqual(ch.qos.logback.classic.Level.WARN))
        .map(layout::doLayout)
        .toList();
  }

  /**
   * A runner whose store throws on load, so {@code processTimedOutSaga} reaches its catch block.
   */
  private static SagaTimeoutRunner<OrderState> runnerWithFailingStore() {
    SagaStore store = mock(SagaStore.class);
    when(store.load(any(), any(), any()))
        .thenThrow(new IllegalStateException("saga store unavailable"));
    return SagaTimeoutRunner.<OrderState>builder()
        .decider(new TimeoutDecider())
        .sagaStore(store)
        .commandBus(new NoopCommandBus())
        .sagaType(SagaType.fromClass(OrderState.class))
        .build();
  }

  @Test
  void processingFailureWarningDoesNotEmitAForgedRecord() {
    SagaId forged = SagaId.of("order-1" + FORGED_TAIL);
    Supplier<List<String>> run =
        () -> renderedWarningsDuring(() -> runnerWithFailingStore().processTimedOutSaga(forged));

    List<String> rendered = run.get();

    assertEquals(1, rendered.size(), "the processing failure must emit exactly one WARNING");
    String record = rendered.get(0);
    // The layout renders one event as exactly one line. Anything beyond that came out of the
    // identifier.
    assertEquals(
        1,
        record.strip().lines().count(),
        "a stored sagaId must not be able to open a further log line: <" + record + ">");
    assertFalse(
        record.contains("\nWARN SagaTimeoutRunner: Saga order-victim"),
        "the forged record must never start a line of its own: <" + record + ">");
    assertTrue(record.contains("order-1"), "the real id must still be present, just declawed");
  }

  @Test
  void processingFailureWarningIsBoundedForAPathologicallyLongId() {
    // SagaId permits 255 characters (it mirrors the saga_state column); a value near that ceiling
    // must not inflate the log line, which is the log-volume amplification arm of the finding.
    SagaId longId = SagaId.of("o".repeat(255));

    List<String> rendered =
        renderedWarningsDuring(() -> runnerWithFailingStore().processTimedOutSaga(longId));

    assertEquals(1, rendered.size());
    assertTrue(
        rendered.get(0).length() < 300,
        "a 255-character saga id must be truncated at the sink: " + rendered.get(0).length());
    assertTrue(rendered.get(0).contains("..."), "truncation must be visible, not silent");
  }
}
