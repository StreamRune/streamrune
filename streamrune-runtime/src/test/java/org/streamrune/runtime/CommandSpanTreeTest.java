package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.LockException;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.SnapshotPolicy;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.InMemoryEventStore;

/**
 * The span tree of one command on the in-memory store, recorded through {@link
 * OpenTelemetryTracing}: {@code command/Switch} holds {@code lock/acquire}, {@code eventstore/load}
 * (with {@code eventstore/snapshot/load} and {@code eventstore/load/deserialize}) and {@code
 * eventstore/append} (with {@code eventstore/append/serialize}); each area switched off removes its
 * spans alone. Also the {@code lock/acquire} span of {@link LocalStripedLocker} and its outcomes.
 */
class CommandSpanTreeTest {

  private static final AggregateType LAMP = AggregateType.of("lamp");

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  record Switch(String lampId) implements Command {}

  record Switched(String lampId) implements DomainEvent {}

  record LampState(int switches) implements AggregateState {}

  static final class LampDecider implements Decider<Switch, LampState, Switched> {
    @Override
    public LampState initialState() {
      return new LampState(0);
    }

    @Override
    public List<Switched> decide(Switch command, LampState state) {
      return List.of(new Switched(command.lampId()));
    }

    @Override
    public LampState evolve(LampState state, Switched event) {
      return new LampState(state.switches() + 1);
    }
  }

  private static StreamRuneTracing tracing(TracingSettings settings) {
    return OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), settings);
  }

  private static void runCommand(StreamRuneTracing tracing) {
    InMemoryEventStore store =
        new InMemoryEventStore(
                SimpleEventTypeRegistry.builder()
                    .registerEvent("Switched", Switched.class)
                    .registerState("LampState", LampState.class)
                    .build(),
                null)
            .withTracing(tracing);
    try (var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .snapshotPolicy(SnapshotPolicy.everyNEvents(1))
            .register(LAMP, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
            .tracing(tracing)
            .build()) {
      // The first command writes the stream's first snapshot; the second loads from it.
      bus.execute(new Switch("l-1"));
      bus.execute(new Switch("l-1"));
    }
  }

  /** The spans of the second command's trace, the one that loads from the snapshot. */
  private static List<SpanData> secondCommand() {
    List<SpanData> commands =
        otelTesting.getSpans().stream().filter(s -> s.getName().equals("command/Switch")).toList();
    assertThat(commands).hasSize(2);
    String traceId = commands.get(1).getTraceId();
    return otelTesting.getSpans().stream().filter(s -> s.getTraceId().equals(traceId)).toList();
  }

  private static SpanData in(List<SpanData> spans, String name) {
    List<SpanData> named = spans.stream().filter(s -> s.getName().equals(name)).toList();
    assertThat(named).as("spans named %s", name).hasSize(1);
    return named.getFirst();
  }

  private static SpanData span(String name) {
    List<SpanData> spans =
        otelTesting.getSpans().stream().filter(s -> s.getName().equals(name)).toList();
    assertThat(spans).as("spans named %s", name).hasSize(1);
    return spans.getFirst();
  }

  private static List<String> names() {
    return otelTesting.getSpans().stream().map(SpanData::getName).toList();
  }

  @Test
  void aCommand_nestsTheLockTheLoadAndTheAppendSpans() {
    runCommand(tracing(TracingSettings.allEnabled()));

    List<SpanData> trace = secondCommand();
    SpanData command = in(trace, "command/Switch");
    SpanData lock = in(trace, TraceAttributes.LOCK_ACQUIRE);
    SpanData load = in(trace, TraceAttributes.EVENT_STORE_LOAD);
    SpanData snapshot = in(trace, TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD);
    SpanData deserialize = in(trace, TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    SpanData append = in(trace, TraceAttributes.EVENT_STORE_APPEND);
    SpanData serialize = in(trace, TraceAttributes.EVENT_STORE_APPEND_SERIALIZE);
    SpanData save = in(trace, TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE);

    assertThat(trace).hasSize(8);
    assertThat(command.getParentSpanContext().isValid()).isFalse();
    for (SpanData child : List.of(lock, load, append, save)) {
      assertThat(child.getParentSpanId()).as(child.getName()).isEqualTo(command.getSpanId());
    }
    assertThat(snapshot.getParentSpanId()).isEqualTo(load.getSpanId());
    assertThat(deserialize.getParentSpanId()).isEqualTo(load.getSpanId());
    assertThat(serialize.getParentSpanId()).isEqualTo(append.getSpanId());
    assertThat(EventStoreTracingTest.string(snapshot, TraceAttributes.SNAPSHOT_OUTCOME))
        .isEqualTo("HIT");
    // In order: the lock, then the load, then the append, then the snapshot.
    assertThat(lock.getEndEpochNanos()).isLessThanOrEqualTo(load.getStartEpochNanos());
    assertThat(load.getEndEpochNanos()).isLessThanOrEqualTo(append.getStartEpochNanos());
    assertThat(append.getEndEpochNanos()).isLessThanOrEqualTo(save.getStartEpochNanos());
    assertThat(EventStoreTracingTest.string(lock, TraceAttributes.LOCK_OUTCOME))
        .isEqualTo("ACQUIRED");

    // The first command's stream had no snapshot yet: its load starts no snapshot span.
    assertThat(names().stream().filter(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD::equals))
        .hasSize(1);
  }

  @Test
  void eachAreaSwitchedOff_removesItsSpansAlone() {
    runCommand(tracing(TracingSettings.builder().area(TracingArea.EVENT_STORE, false).build()));
    assertThat(names())
        .containsExactlyInAnyOrder(
            TraceAttributes.LOCK_ACQUIRE,
            "command/Switch",
            TraceAttributes.LOCK_ACQUIRE,
            "command/Switch");
    otelTesting.clearSpans();

    runCommand(tracing(TracingSettings.builder().area(TracingArea.LOCK, false).build()));
    assertThat(names())
        .contains("command/Switch", TraceAttributes.EVENT_STORE_APPEND)
        .doesNotContain(TraceAttributes.LOCK_ACQUIRE);
    otelTesting.clearSpans();

    runCommand(tracing(TracingSettings.builder().area(TracingArea.COMMAND, false).build()));
    assertThat(names())
        .contains(TraceAttributes.LOCK_ACQUIRE, TraceAttributes.EVENT_STORE_LOAD)
        .doesNotContain("command/Switch");
    otelTesting.clearSpans();

    runCommand(tracing(TracingSettings.disabled()));
    runCommand(StreamRuneTracing.NOOP);
    assertThat(otelTesting.getSpans()).isEmpty();
  }

  // ==================== LocalStripedLocker ====================

  private static final StreamId STREAM = StreamId.of(LAMP, AggregateId.of("l-1"));

  @Test
  void lockAcquire_carriesTheStreamAndTheOutcome() throws Exception {
    LocalStripedLocker locker =
        new LocalStripedLocker(1).traced(tracing(TracingSettings.allEnabled()));

    try (AutoCloseable lock = locker.acquireLock(STREAM, Duration.ofSeconds(1))) {
      SpanData acquired = span(TraceAttributes.LOCK_ACQUIRE);
      assertThat(acquired.getKind()).isEqualTo(SpanKind.INTERNAL);
      assertThat(EventStoreTracingTest.keys(acquired))
          .containsExactlyInAnyOrder(TraceAttributes.STREAM_ID, TraceAttributes.LOCK_OUTCOME);
      assertThat(EventStoreTracingTest.string(acquired, TraceAttributes.STREAM_ID))
          .isEqualTo("lamp:l-1");
      assertThat(EventStoreTracingTest.string(acquired, TraceAttributes.LOCK_OUTCOME))
          .isEqualTo("ACQUIRED");
      assertThat(acquired.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
    }
  }

  @Test
  void aLockHeldElsewhere_isTheOutcomeTimeout_andAnInterruptedWaitTheOutcomeInterrupted()
      throws Exception {
    LocalStripedLocker untraced = new LocalStripedLocker(1);
    LocalStripedLocker locker = untraced.traced(tracing(TracingSettings.allEnabled()));
    CountDownLatch held = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    Thread holder =
        Thread.ofVirtual()
            .start(
                () -> {
                  try (AutoCloseable lock = untraced.acquireLock(STREAM, Duration.ofSeconds(1))) {
                    held.countDown();
                    release.await();
                  } catch (Exception e) {
                    throw new IllegalStateException(e);
                  }
                });
    try {
      assertThat(held.await(5, TimeUnit.SECONDS)).isTrue();
      // The two lockers share their stripes: the lock the untraced one holds is held here too.
      assertThatThrownBy(() -> locker.acquireLock(STREAM, Duration.ZERO))
          .isInstanceOf(LockException.class);
      SpanData timedOut = span(TraceAttributes.LOCK_ACQUIRE);
      assertThat(EventStoreTracingTest.string(timedOut, TraceAttributes.LOCK_OUTCOME))
          .isEqualTo("TIMEOUT");
      assertThat(timedOut.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
      otelTesting.clearSpans();

      Thread.currentThread().interrupt();
      assertThatThrownBy(() -> locker.acquireLock(STREAM, Duration.ofSeconds(1)))
          .isInstanceOf(LockException.class);
      assertThat(Thread.interrupted()).isTrue();
      SpanData interrupted = span(TraceAttributes.LOCK_ACQUIRE);
      assertThat(EventStoreTracingTest.string(interrupted, TraceAttributes.LOCK_OUTCOME))
          .isEqualTo("INTERRUPTED");
      assertThat(interrupted.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
    } finally {
      release.countDown();
      holder.join();
    }
  }

  @Test
  void anInvalidTimeout_marksTheLockSpanFailed_withoutAnOutcome() {
    LocalStripedLocker locker =
        new LocalStripedLocker(1).traced(tracing(TracingSettings.allEnabled()));

    assertThatThrownBy(() -> locker.acquireLock(STREAM, Duration.ofMillis(-1)))
        .isInstanceOf(IllegalArgumentException.class);

    SpanData failed = span(TraceAttributes.LOCK_ACQUIRE);
    EventStoreTracingTest.assertFailed(failed);
    assertThat(EventStoreTracingTest.keys(failed)).containsExactly(TraceAttributes.STREAM_ID);
  }

  @Test
  void withTheLockAreaOff_orNoop_orUntraced_noLockSpanStarts() throws Exception {
    for (LocalStripedLocker locker :
        List.of(
            new LocalStripedLocker(4),
            new LocalStripedLocker(4).traced(StreamRuneTracing.NOOP),
            new LocalStripedLocker(4)
                .traced(
                    tracing(TracingSettings.builder().area(TracingArea.LOCK, false).build())))) {
      locker.acquireLock(STREAM, Duration.ofSeconds(1)).close();
    }

    assertThat(otelTesting.getSpans()).isEmpty();
    assertThatThrownBy(() -> new LocalStripedLocker(1).traced(null))
        .isInstanceOf(NullPointerException.class);
  }
}
