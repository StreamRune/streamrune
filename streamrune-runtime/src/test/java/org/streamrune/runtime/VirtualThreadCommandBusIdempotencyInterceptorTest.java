package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus.CommandResult;
import org.streamrune.core.CommandBus.ShortCircuitReason;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;

/**
 * Verifies that {@link VirtualThreadCommandBus#execute(Command, IdempotencyKey)} runs the
 * interceptor chain even on an inbox HIT (idempotent replay): authorization/validation/audit
 * interceptors must see every call, not just the one that actually runs the handler. Also verifies
 * that reusing a key across different command types is rejected rather than returning the stored
 * result for the wrong command.
 */
class VirtualThreadCommandBusIdempotencyInterceptorTest {

  private static final AggregateType TYPE = AggregateType.of("widget");

  // === Minimal test domain ===

  record WidgetCommand(String widgetId) implements Command {}

  record OtherCommand(String widgetId) implements Command {}

  sealed interface WidgetEvent extends DomainEvent {
    record WidgetCreated() implements WidgetEvent {}
  }

  record WidgetState(boolean created) implements AggregateState {}

  /** Decider that always appends one event; counts how many times decide() actually ran. */
  static class CountingOneEventDecider implements Decider<WidgetCommand, WidgetState, WidgetEvent> {
    final AtomicInteger decideCalls = new AtomicInteger();

    @Override
    public WidgetState initialState() {
      return new WidgetState(false);
    }

    @Override
    public List<WidgetEvent> decide(WidgetCommand command, WidgetState state) {
      decideCalls.incrementAndGet();
      return List.of(new WidgetEvent.WidgetCreated());
    }

    @Override
    public WidgetState evolve(WidgetState state, WidgetEvent event) {
      return new WidgetState(true);
    }
  }

  /** Records before()/after() invocations; optionally throws on before() or vetoes via before(). */
  static class RecordingInterceptor implements CommandInterceptor {
    final List<String> log = new CopyOnWriteArrayList<>();
    final AtomicInteger beforeCalls = new AtomicInteger();
    final AtomicInteger afterCalls = new AtomicInteger();
    RuntimeException throwOnBeforeCall = null; // thrown on the Nth before() call (1-based), if set
    int throwOnBeforeCallNumber = -1;
    boolean vetoOnSecondCall = false;

    @Override
    public boolean before(CommandContext ctx) {
      int n = beforeCalls.incrementAndGet();
      log.add("before-" + n);
      if (throwOnBeforeCall != null && n == throwOnBeforeCallNumber) {
        throw throwOnBeforeCall;
      }
      if (vetoOnSecondCall && n == 2) {
        return false;
      }
      return true;
    }

    @Override
    public void after(CommandContext ctx) {
      afterCalls.incrementAndGet();
      log.add("after-" + ctx.result().shortCircuited());
    }

    @Override
    public void onError(CommandContext ctx, Throwable error) {
      log.add("onError");
    }
  }

  private static VirtualThreadCommandBus busWithInbox(
      InMemoryEventStore store,
      InMemoryCommandInbox inbox,
      Decider<WidgetCommand, WidgetState, WidgetEvent> decider,
      CommandInterceptor... interceptors) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .commandInbox(inbox)
        .interceptors(interceptors)
        .register(TYPE, WidgetCommand.class, cmd -> AggregateId.of(cmd.widgetId()), decider)
        .build();
  }

  @Test
  void replay_runsInterceptors() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var decider = new CountingOneEventDecider();
    var interceptor = new RecordingInterceptor();
    var bus = busWithInbox(store, inbox, decider, interceptor);

    var cmd = new WidgetCommand("w-1");
    var key = IdempotencyKey.of("key-1");

    CommandResult first = bus.execute(cmd, key);
    CommandResult second = bus.execute(cmd, key);

    assertFalse(first.shortCircuited(), "first execution must not be short-circuited");
    assertEquals(ShortCircuitReason.NONE, first.reason());
    assertTrue(second.shortCircuited(), "second execution (replay) must be short-circuited");
    assertEquals(key.value(), second.shortCircuitedBy(), "replay must be marked by the key value");
    // The replay is a SUCCESS, not a rejection: idempotentReplay() true, vetoed() false.
    assertEquals(ShortCircuitReason.IDEMPOTENT_REPLAY, second.reason());
    assertTrue(second.idempotentReplay(), "a pure inbox replay is an IDEMPOTENT_REPLAY success");
    assertFalse(second.vetoed(), "an idempotent replay must never be reported as a rejection");
    // Events are persisted (by the FIRST execution) — the replay returns that prior outcome.
    assertEquals(
        first.streamId(), second.streamId(), "replay carries the original success outcome");
    assertFalse(
        store.load(first.streamId(), 0).events().isEmpty(),
        "the events the replay refers to are persisted in the store");

    assertEquals(1, decider.decideCalls.get(), "handler/decider must run exactly once");
    assertEquals(2, interceptor.beforeCalls.get(), "interceptor must see before() on both calls");
    assertEquals(2, interceptor.afterCalls.get(), "interceptor must see after() on both calls");
  }

  @Test
  void replay_deniedByAuthInterceptor_doesNotLeakStoredResult() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var decider = new CountingOneEventDecider();
    var interceptor = new RecordingInterceptor();
    interceptor.throwOnBeforeCall = new SecurityException("not authorized");
    interceptor.throwOnBeforeCallNumber = 2;
    var bus = busWithInbox(store, inbox, decider, interceptor);

    var cmd = new WidgetCommand("w-2");
    var key = IdempotencyKey.of("key-2");

    bus.execute(cmd, key);
    assertThrows(SecurityException.class, () -> bus.execute(cmd, key));

    assertEquals(1, decider.decideCalls.get(), "handler must not run again on the denied replay");
  }

  @Test
  void replay_vetoingInterceptor_winsOverInbox() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var decider = new CountingOneEventDecider();
    var interceptor = new RecordingInterceptor();
    interceptor.vetoOnSecondCall = true;
    var bus = busWithInbox(store, inbox, decider, interceptor);

    var cmd = new WidgetCommand("w-3");
    var key = IdempotencyKey.of("key-3");

    CommandResult first = bus.execute(cmd, key);
    CommandResult second = bus.execute(cmd, key);

    assertFalse(first.shortCircuited());
    assertTrue(second.shortCircuited(), "vetoed call must be short-circuited");
    // The veto is a REJECTION (VETOED), NOT an idempotent replay — even though an inbox row exists.
    assertEquals(
        ShortCircuitReason.VETOED,
        second.reason(),
        "veto must win over the inbox replay — reason is VETOED (a rejection), not IDEMPOTENT_REPLAY");
    assertTrue(second.vetoed());
    assertFalse(second.idempotentReplay(), "a veto is not a success replay");
    assertEquals(
        "RecordingInterceptor",
        second.shortCircuitedBy(),
        "veto must win over the inbox replay — shortCircuitedBy must name the interceptor, not the key");
    assertNotEquals(key.value(), second.shortCircuitedBy());
    assertEquals(1, decider.decideCalls.get(), "handler must not run on the vetoed call");
  }

  /** A CommandInbox whose find() always fails, simulating a DB blip during the inbox pre-check. */
  static final class ThrowingFindInbox implements CommandInbox {
    @Override
    public Optional<InboxResult> find(IdempotencyKey key) {
      throw new IllegalStateException("inbox unreachable");
    }

    @Override
    public int deleteProcessedBefore(Instant cutoff) {
      return 0;
    }
  }

  /** Counts recordCommandFailed() invocations. */
  static final class CountingMetrics implements StreamRuneMetrics {
    final AtomicInteger failed = new AtomicInteger();

    @Override
    public void recordCommandFailed() {
      failed.incrementAndGet();
    }
  }

  @Test
  void inboxPreCheckFailure_routesThroughOnErrorAndFailureMetric() {
    // A keyed command whose inbox pre-check (commandInbox.find(key)) throws must be routed
    // through the SAME interceptor/metric error path as a handler failure — onError() fires on
    // every interceptor whose before() completed (never after()), and recordCommandFailed() is
    // recorded. Otherwise the OTel scope + audit row from outer interceptors leak and the failure
    // is invisible to metrics precisely during an infra incident.
    var store = new InMemoryEventStore();
    var metrics = new CountingMetrics();
    var interceptor = new RecordingInterceptor();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(new ThrowingFindInbox())
            .interceptors(interceptor)
            .metrics(metrics)
            .register(
                TYPE,
                WidgetCommand.class,
                cmd -> AggregateId.of(cmd.widgetId()),
                new CountingOneEventDecider())
            .build();

    assertThrows(
        IllegalStateException.class,
        () -> bus.execute(new WidgetCommand("w-fail"), IdempotencyKey.of("key-fail")));

    assertTrue(
        interceptor.log.contains("onError"), "onError() must fire on the inbox pre-check failure");
    assertEquals(
        0, interceptor.afterCalls.get(), "after() must NOT be called when the command failed");
    assertEquals(1, metrics.failed.get(), "recordCommandFailed() must be recorded on the failure");
  }

  @Test
  void keyReuseAcrossCommandTypes_throws() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var decider = new CountingOneEventDecider();

    var busForWidget =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(inbox)
            .register(TYPE, WidgetCommand.class, cmd -> AggregateId.of(cmd.widgetId()), decider)
            .register(
                TYPE,
                OtherCommand.class,
                cmd -> AggregateId.of(cmd.widgetId()),
                new Decider<OtherCommand, WidgetState, WidgetEvent>() {
                  @Override
                  public WidgetState initialState() {
                    return new WidgetState(false);
                  }

                  @Override
                  public List<WidgetEvent> decide(OtherCommand command, WidgetState state) {
                    return List.of(new WidgetEvent.WidgetCreated());
                  }

                  @Override
                  public WidgetState evolve(WidgetState state, WidgetEvent event) {
                    return new WidgetState(true);
                  }
                })
            .build();

    var key = IdempotencyKey.of("shared-key");
    busForWidget.execute(new WidgetCommand("w-4"), key);

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> busForWidget.execute(new OtherCommand("w-4"), key));
    assertTrue(
        ex.getMessage().contains("shared-key"),
        "exception must name the offending key, got: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("WidgetCommand") && ex.getMessage().contains("OtherCommand"),
        "exception must name both the stored and presented command types, got: " + ex.getMessage());
  }

  /**
   * The RACE WINDOW of {@link #keyReuseAcrossCommandTypes_throws}. Two concurrent executions both
   * run {@code commandInbox.find(key)} before either has committed its inbox row, so both MISS the
   * bus's sequential pre-check and both reach {@code EventStore.appendWithKey}. The window is
   * simulated deterministically by giving the bus an inbox whose {@code find()} always misses while
   * the event store's own inbox already holds the winner's row — exactly the state the loser
   * observes at claim time.
   *
   * <p>Before the fix the store's claim-lost branch returned the winner's offsets and final version
   * with {@code alreadyApplied=true}, and the bus turned that straight into an {@code
   * IDEMPOTENT_REPLAY} success: the caller was told "already applied, HTTP 200" for a command that
   * never ran and never will, and the response carried another aggregate's stream metadata. Same
   * two inputs as the sequential test, opposite outcome, decided purely by timing.
   */
  @Test
  void raceWindow_keyReuseAcrossCommandTypes_doesNotReturnIdempotentReplay() {
    var realInbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(realInbox);
    // The bus sees a permanently-missing inbox (the pre-commit race window); the store sees the
    // real one, which is where the winner's row actually lands.
    CommandInbox blindInbox =
        new CommandInbox() {
          @Override
          public Optional<InboxResult> find(IdempotencyKey key) {
            return Optional.empty();
          }

          @Override
          public int deleteProcessedBefore(Instant cutoff) {
            return realInbox.deleteProcessedBefore(cutoff);
          }
        };

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(blindInbox)
            .register(
                TYPE,
                WidgetCommand.class,
                cmd -> AggregateId.of(cmd.widgetId()),
                new CountingOneEventDecider())
            .register(
                TYPE,
                OtherCommand.class,
                cmd -> AggregateId.of(cmd.widgetId()),
                new Decider<OtherCommand, WidgetState, WidgetEvent>() {
                  @Override
                  public WidgetState initialState() {
                    return new WidgetState(false);
                  }

                  @Override
                  public List<WidgetEvent> decide(OtherCommand command, WidgetState state) {
                    return List.of(new WidgetEvent.WidgetCreated());
                  }

                  @Override
                  public WidgetState evolve(WidgetState state, WidgetEvent event) {
                    return new WidgetState(true);
                  }
                })
            .build();

    var key = IdempotencyKey.of("race-shared-key");
    CommandResult winner = bus.execute(new WidgetCommand("w-race"), key);
    assertEquals(ShortCircuitReason.NONE, winner.reason(), "the winner executed for real");
    assertTrue(realInbox.find(key).isPresent(), "the winner's inbox row is committed");

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> bus.execute(new OtherCommand("w-race"), key),
            "a racing key collision must fail exactly like the sequential one, never"
                + " short-circuit as a success");
    assertTrue(
        ex.getMessage().contains("race-shared-key"),
        "exception must name the offending key, got: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("WidgetCommand") && ex.getMessage().contains("OtherCommand"),
        "exception must name both the stored and presented command types, got: " + ex.getMessage());

    assertEquals(
        1,
        store
            .readStream(StreamId.of(TYPE, AggregateId.of("w-race")), Version.initial(), 100)
            .size(),
        "only the winner's event may be persisted");
  }
}
