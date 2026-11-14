package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.ThrowingTracing.Fault;
import org.streamrune.test.InMemoryEventStore;

/**
 * The traced {@link LocalStripedLocker} never leaks the lock it acquired when its tracing throws:
 * the caller gets the lock, releases it, and every other thread can take the stream afterwards.
 */
class TracedLockerLeakTest {

  private static final AggregateType LAMP = AggregateType.of("lamp");
  private static final StreamId LAMP_1 = StreamId.of(LAMP, AggregateId.of("l-1"));

  /** The tracing failures that hit a locker: before the acquisition and after it. */
  static Stream<ThrowingTracing> brokenTracings() {
    return Stream.of(
        ThrowingTracing.on(Fault.START),
        ThrowingTracing.on(Fault.SET_ATTRIBUTE),
        ThrowingTracing.on(Fault.RECORD_SUCCESS),
        ThrowingTracing.on(Fault.CLOSE),
        ThrowingTracing.on(
            Fault.START,
            Fault.SET_ATTRIBUTE,
            Fault.RECORD_SUCCESS,
            Fault.RECORD_ERROR,
            Fault.CLOSE),
        ThrowingTracing.onEveryScopeMethod(),
        ThrowingTracing.throwing(
            () -> new OutOfMemoryError("in the span's end"), Fault.RECORD_SUCCESS, Fault.CLOSE));
  }

  /** Runs {@code work} on a thread of its own, so a lock it leaves held is another thread's. */
  private static <T> T onItsOwnThread(Callable<T> work) throws Exception {
    AtomicReference<T> result = new AtomicReference<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    Thread thread =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    result.set(work.call());
                  } catch (Throwable t) {
                    failure.set(t);
                  }
                });
    thread.join(30_000);
    if (failure.get() != null) {
      throw new IllegalStateException(failure.get());
    }
    return result.get();
  }

  private static Throwable acquireAndRelease(LocalStripedLocker locker, StreamId stream) {
    try (AutoCloseable lock = locker.acquireLock(stream, Duration.ofMillis(300))) {
      assertThat(lock).isNotNull();
      return null;
    } catch (Throwable t) {
      return t;
    }
  }

  @ParameterizedTest
  @MethodSource("brokenTracings")
  void theCallerGetsTheLock_andEveryOtherThreadCanTakeTheStreamAfterwards(ThrowingTracing broken)
      throws Exception {
    LocalStripedLocker plain = new LocalStripedLocker(16);
    LocalStripedLocker traced = plain.traced(broken);

    for (int i = 0; i < 20; i++) {
      assertThat(onItsOwnThread(() -> acquireAndRelease(traced, LAMP_1)))
          .as("acquisition %s through the traced locker", i)
          .isNull();
    }
    assertThat(broken.thrown.get()).as("the tracing did throw").isPositive();
    assertThat(onItsOwnThread(() -> acquireAndRelease(plain, LAMP_1)))
        .as("the untraced locker on the same stripes")
        .isNull();
  }

  @ParameterizedTest
  @MethodSource("brokenTracings")
  void aLockThatIsHeldIsStillReportedAsATimeout(ThrowingTracing broken) throws Exception {
    LocalStripedLocker plain = new LocalStripedLocker(16);
    LocalStripedLocker traced = plain.traced(broken);

    try (AutoCloseable held = plain.acquireLock(LAMP_1, Duration.ofMillis(300))) {
      Throwable refused = onItsOwnThread(() -> acquireAndRelease(traced, LAMP_1));
      assertThat(refused)
          .as("the true outcome: the lock was not acquired")
          .isInstanceOf(LockAcquisitionException.class);
    }
    assertThat(onItsOwnThread(() -> acquireAndRelease(traced, LAMP_1))).isNull();
  }

  // ── through the command bus ────────────────────────────────────────────────────────────────

  private static final org.streamrune.core.EventTypeRegistry TYPES =
      org.streamrune.core.SimpleEventTypeRegistry.builder()
          .registerEvent("Switched", Switched.class)
          .registerState("Lamp", Lamp.class)
          .build();

  record Switched(String lampId) implements DomainEvent {}

  record Lamp() implements AggregateState {}

  record Switch(String lampId) implements Command {}

  static final class LampDecider implements Decider<Switch, Lamp, Switched> {
    @Override
    public Lamp initialState() {
      return new Lamp();
    }

    @Override
    public List<Switched> decide(Switch command, Lamp state) {
      return List.of(new Switched(command.lampId()));
    }

    @Override
    public Lamp evolve(Lamp state, Switched event) {
      return state;
    }
  }

  @ParameterizedTest
  @MethodSource("brokenTracings")
  void everyCommandOnTheAggregateSucceeds_fromAnyThread(ThrowingTracing broken) throws Exception {
    InMemoryEventStore store = new InMemoryEventStore(TYPES);
    try (VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16).traced(broken))
            .lockTimeout(Duration.ofMillis(300))
            .register(LAMP, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
            .tracing(broken)
            .build()) {
      for (int i = 0; i < 5; i++) {
        Throwable failure =
            onItsOwnThread(
                () -> {
                  try {
                    bus.execute(new Switch("l-1"));
                    return null;
                  } catch (Throwable t) {
                    return t;
                  }
                });
        assertThat(failure).as("command %s on l-1", i).isNull();
      }
      assertThat(store.readStream(LAMP_1, Version.initial(), 100)).hasSize(5);
    }
  }

  @ParameterizedTest
  @MethodSource("brokenTracings")
  void anInvalidTimeoutIsStillRejected(ThrowingTracing broken) {
    LocalStripedLocker traced = new LocalStripedLocker(16).traced(broken);
    assertThatThrownBy(() -> traced.acquireLock(LAMP_1, Duration.ofMillis(-1)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
