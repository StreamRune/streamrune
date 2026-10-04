package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus.CommandResult;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;

/**
 * Verifies {@link VirtualThreadCommandBus#execute(Command, IdempotencyKey)}: inbox pre-check, keyed
 * atomic append, zero-event key recording, and missing-inbox guard.
 */
class VirtualThreadCommandBusIdempotencyTest {

  private static final AggregateType TYPE = AggregateType.of("widget");

  // === Minimal test domain ===

  record WidgetCommand(String widgetId) implements Command {}

  sealed interface WidgetEvent extends DomainEvent {
    record WidgetCreated() implements WidgetEvent {}
  }

  record WidgetState(boolean created) implements AggregateState {}

  /** Decider that always appends one event. */
  static class OneEventDecider implements Decider<WidgetCommand, WidgetState, WidgetEvent> {
    @Override
    public WidgetState initialState() {
      return new WidgetState(false);
    }

    @Override
    public List<WidgetEvent> decide(WidgetCommand command, WidgetState state) {
      return List.of(new WidgetEvent.WidgetCreated());
    }

    @Override
    public WidgetState evolve(WidgetState state, WidgetEvent event) {
      return new WidgetState(true);
    }
  }

  /** Decider that always returns zero events. */
  static class ZeroEventDecider implements Decider<WidgetCommand, WidgetState, WidgetEvent> {
    @Override
    public WidgetState initialState() {
      return new WidgetState(false);
    }

    @Override
    public List<WidgetEvent> decide(WidgetCommand command, WidgetState state) {
      return List.of();
    }

    @Override
    public WidgetState evolve(WidgetState state, WidgetEvent event) {
      return state;
    }
  }

  private static VirtualThreadCommandBus busWithInbox(
      InMemoryEventStore store, InMemoryCommandInbox inbox, Decider<?, ?, ?> decider) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .commandInbox(inbox)
        .register(
            TYPE,
            WidgetCommand.class,
            cmd -> AggregateId.of(cmd.widgetId()),
            (Decider<WidgetCommand, WidgetState, WidgetEvent>) decider)
        .build();
  }

  // === Tests ===

  /**
   * execute(cmd, key) twice → the event stream grows by exactly one; the 2nd result
   * shortCircuited() is true and carries the same globalOffsets.
   */
  @Test
  void idempotentExecute_secondCallShortCircuits_streamGrowsByOne() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var bus = busWithInbox(store, inbox, new OneEventDecider());

    var cmd = new WidgetCommand("w-1");
    var key = IdempotencyKey.of("key-1");
    var streamId = StreamId.of(TYPE, AggregateId.of("w-1"));

    CommandResult first = bus.execute(cmd, key);
    assertFalse(first.shortCircuited(), "first execution must not be short-circuited");
    assertEquals(1, first.globalOffsets().size(), "first call must produce one event");

    CommandResult second = bus.execute(cmd, key);
    assertTrue(second.shortCircuited(), "second execution must be short-circuited");
    assertEquals(
        first.globalOffsets(),
        second.globalOffsets(),
        "second result must carry the same global offsets");

    // Stream must have exactly one event
    var events = store.readStream(streamId, org.streamrune.core.types.Version.initial(), 100);
    assertEquals(1, events.size(), "stream must contain exactly one event");
  }

  /** The replayed result of a keyed duplicate carries the same typed stream as the first result. */
  @Test
  void keyedReplay_carriesTheTypedStream() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var bus = busWithInbox(store, inbox, new OneEventDecider());

    var cmd = new WidgetCommand("w-typed");
    var key = IdempotencyKey.of("k-typed");
    var expected = StreamId.of(TYPE, AggregateId.of("w-typed"));

    CommandResult first = bus.execute(cmd, key);
    CommandResult replayed = bus.execute(cmd, key);

    assertFalse(first.shortCircuited());
    assertEquals(expected, first.streamId());
    assertTrue(replayed.idempotentReplay(), "the second keyed call is answered from the inbox");
    assertEquals(
        expected, replayed.streamId(), "the replay names the typed stream, never a bare id");
  }

  /** execute(cmd) (no key) twice → stream grows by two (unchanged no-key behavior). */
  @Test
  void unkeyedExecute_calledTwice_streamGrowsByTwo() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var bus = busWithInbox(store, inbox, new OneEventDecider());

    var cmd = new WidgetCommand("w-2");
    var streamId = StreamId.of(TYPE, AggregateId.of("w-2"));

    bus.execute(cmd);
    bus.execute(cmd);

    var events = store.readStream(streamId, org.streamrune.core.types.Version.initial(), 100);
    assertEquals(2, events.size(), "unkeyed calls must each append independently");
  }

  /** Zero-event decider + keyed execute → 2nd call short-circuits; inbox.find(key) is present. */
  @Test
  void zeroEventDecider_keyedExecute_keyRecordedAndSecondShortCircuits() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var bus = busWithInbox(store, inbox, new ZeroEventDecider());

    var cmd = new WidgetCommand("w-3");
    var key = IdempotencyKey.of("key-zero");

    CommandResult first = bus.execute(cmd, key);
    assertFalse(first.shortCircuited(), "first zero-event keyed execute is not short-circuited");
    assertTrue(inbox.find(key).isPresent(), "inbox must record the key after zero-event execute");

    CommandResult second = bus.execute(cmd, key);
    assertTrue(second.shortCircuited(), "second zero-event keyed execute must short-circuit");
  }

  /**
   * A second caller presenting someone else's key with the SAME command type must not be handed the
   * first caller's aggregate.
   *
   * <p>The framework does not bind inbox rows to a principal (see {@link IdempotencyKey}: the same
   * logical command legitimately arrives under different or absent request contexts). What it does
   * enforce is that a key belongs to one command on one stream — so a colliding key presented
   * against a different aggregate is rejected rather than replaying the first caller's {@code
   * streamId}, {@code finalVersion} and offsets back to the second. Isolating callers whose
   * commands DO target the same stream is the application's job, via {@link
   * IdempotencyKey#scopedTo} (next test) plus its authorization interceptors, which run on a replay
   * exactly as on a first execution.
   */
  @Test
  void keyCollisionAcrossCallers_differentAggregate_isRejectedNotReplayed() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var bus = busWithInbox(store, inbox, new OneEventDecider());

    var sharedKey = IdempotencyKey.of("order-create-12345");
    CommandResult alice = bus.execute(new WidgetCommand("order-alice-999"), sharedKey);
    assertEquals(StreamId.of(TYPE, AggregateId.of("order-alice-999")), alice.streamId());

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> bus.execute(new WidgetCommand("order-bob-111"), sharedKey),
            "a colliding key on another aggregate must not replay the first caller's result");
    assertTrue(ex.getMessage().contains("order-alice-999"), ex.getMessage());
    assertTrue(ex.getMessage().contains("order-bob-111"), ex.getMessage());

    assertTrue(
        store
            .readStream(
                StreamId.of(TYPE, AggregateId.of("order-bob-111")),
                org.streamrune.core.types.Version.initial(),
                10)
            .isEmpty(),
        "the rejected command appended nothing");
  }

  /**
   * The supported remedy for keys derived from client input: two principals sending the same {@code
   * Idempotency-Key} header land in separate namespaces, so both commands execute independently and
   * neither sees the other's result.
   */
  @Test
  void scopedKeys_isolateCallersSharingOneClientSuppliedKey() {
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    var bus = busWithInbox(store, inbox, new OneEventDecider());

    String clientHeader = "order-create-12345";
    CommandResult alice =
        bus.execute(
            new WidgetCommand("order-alice-999"), IdempotencyKey.scopedTo("alice", clientHeader));
    CommandResult bob =
        bus.execute(
            new WidgetCommand("order-bob-111"), IdempotencyKey.scopedTo("bob", clientHeader));

    assertFalse(alice.idempotentReplay(), "alice's command executed");
    assertFalse(bob.idempotentReplay(), "bob's command executed — it is not a replay of alice's");
    assertEquals(StreamId.of(TYPE, AggregateId.of("order-alice-999")), alice.streamId());
    assertEquals(StreamId.of(TYPE, AggregateId.of("order-bob-111")), bob.streamId());
    assertNotEquals(alice.globalOffsets(), bob.globalOffsets());
  }

  /** execute(cmd, key) with no inbox configured → IllegalStateException. */
  @Test
  void executeWithKey_noInboxConfigured_throwsIllegalStateException() {
    var store = new InMemoryEventStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .register(
                TYPE,
                WidgetCommand.class,
                cmd -> AggregateId.of(cmd.widgetId()),
                new OneEventDecider())
            .build();

    var cmd = new WidgetCommand("w-4");
    var key = IdempotencyKey.of("key-no-inbox");

    assertThrows(IllegalStateException.class, () -> bus.execute(cmd, key));
  }
}
