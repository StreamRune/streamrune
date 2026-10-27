package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.FaultSchedule;
import org.streamrune.test.ForwardingDeadLetterQueue;
import org.streamrune.test.ForwardingEventStore;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InjectedFaultException;

/**
 * A command whose dead-letter entry is stored has a pending outcome, and the caller of {@code
 * execute} learns it: the bus throws {@link CommandDeadLetteredException} with the entry's ids in
 * place of the failure, which is its cause. A caller that took the failure for final and issued the
 * operation again under a new key would have it applied twice once the entry is replayed under the
 * original key.
 *
 * <p>Where no entry is stored the failure propagates unchanged — a failed publish, a business
 * rejection, the replay of an entry, a command a saga dispatches — and so does the failure of an
 * {@code executeAsync} future and the failure the interceptors see.
 */
class CommandDeadLetteredExceptionTest {

  private static final AggregateType COUNTER = AggregateType.of("counter");

  private final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
  private final AtomicReference<EventStoreException> storeFailure = new AtomicReference<>();
  private final ForwardingEventStore store =
      new ForwardingEventStore(
          new InMemoryEventStore().withCommandInbox(inbox),
          FaultSchedule.none()
              .throwing(
                  () -> {
                    var failure = new EventStoreException("connection reset by peer");
                    storeFailure.set(failure);
                    return failure;
                  })
              .failAlways("append")
              .failAlways("appendWithKey"));
  private final InMemoryDeadLetterQueue deadLetters = new InMemoryDeadLetterQueue();
  private final RecordingInterceptor interceptor = new RecordingInterceptor();

  @Test
  void aKeyedCommandWhoseEntryIsStored_isAPendingOutcomeCarryingTheEntrysIds() {
    var bus = bus(deadLetters);
    var command = new Adjust("c-1", 50);
    var key = IdempotencyKey.of("client-operation-1");

    Throwable thrown = catchThrowable(() -> bus.execute(command, key));

    assertThat(thrown).isInstanceOf(CommandDeadLetteredException.class);
    var deadLettered = (CommandDeadLetteredException) thrown;
    DeadLetterQueue.DeadLetterEntry entry = deadLetters.all().getFirst();
    assertThat(deadLetters.all()).hasSize(1);
    assertThat(deadLettered.commandId()).isEqualTo(entry.commandId());
    assertThat(deadLettered.streamId())
        .isEqualTo(entry.streamId())
        .isEqualTo(StreamId.of(COUNTER, AggregateId.of("c-1")));
    assertThat(deadLettered.idempotencyKey()).isEqualTo(key).isEqualTo(entry.idempotencyKey());
    assertThat(deadLettered.command()).isEqualTo(command);
    assertThat(deadLettered.getCause()).isSameAs(storeFailure.get());
    assertThat(deadLettered.getMessage())
        .contains("Adjust", entry.commandId().value(), "same key")
        .doesNotContain(key.value());
    assertThat(deadLettered.toString())
        .as("the command's payload is not rendered")
        .doesNotContain("delta=50")
        .startsWith(CommandDeadLetteredException.class.getName() + ": ");
  }

  @Test
  void anUnkeyedCommandWhoseEntryIsStored_isAPendingOutcomeWithoutAKey() {
    var bus = bus(deadLetters);

    Throwable thrown = catchThrowable(() -> bus.execute(new Adjust("c-2", 5)));

    assertThat(thrown).isInstanceOf(CommandDeadLetteredException.class);
    var deadLettered = (CommandDeadLetteredException) thrown;
    DeadLetterQueue.DeadLetterEntry entry = deadLetters.all().getFirst();
    assertThat(deadLettered.commandId()).isEqualTo(entry.commandId());
    assertThat(deadLettered.streamId()).isEqualTo(entry.streamId());
    assertThat(deadLettered.idempotencyKey()).isNull();
    assertThat(entry.idempotencyKey()).isNull();
    assertThat(deadLettered.getCause()).isSameAs(storeFailure.get());
    assertThat(deadLettered.getMessage()).contains("without an idempotency key", "operator");
  }

  @Test
  void theInterceptorsSeeTheFailureItself() {
    var bus = bus(deadLetters);

    Throwable thrown = catchThrowable(() -> bus.execute(new Adjust("c-3", 5)));

    assertThat(thrown).isInstanceOf(CommandDeadLetteredException.class);
    assertThat(interceptor.errors).singleElement().isSameAs(storeFailure.get());
  }

  @Test
  void executeAsync_failsItsFutureWithTheFailureItself_andStoresTheEntry() {
    var bus = bus(deadLetters);

    CompletableFuture<CommandBus.CommandResult> future = bus.executeAsync(new Adjust("c-4", 5));
    Throwable thrown = catchThrowable(future::join);

    assertThat(thrown).isInstanceOf(CompletionException.class);
    assertThat(thrown.getCause()).isSameAs(storeFailure.get());
    assertThat(deadLetters.all())
        .singleElement()
        .satisfies(entry -> assertThat(entry.idempotencyKey()).isNull());
    assertThat(interceptor.errors).singleElement().isSameAs(storeFailure.get());
  }

  @Test
  void aFailedPublish_propagatesTheFailureItself_withThePublishFailureAttached() {
    var bus =
        bus(new ForwardingDeadLetterQueue(deadLetters, FaultSchedule.none().failAlways("publish")));

    Throwable thrown =
        catchThrowable(() -> bus.execute(new Adjust("c-5", 5), IdempotencyKey.of("op-5")));

    assertThat(thrown).isSameAs(storeFailure.get());
    assertThat(thrown.getSuppressed()).singleElement().isInstanceOf(InjectedFaultException.class);
    assertThat(deadLetters.all()).isEmpty();
  }

  @Test
  void aBusinessRejection_isNotDeadLettered_andPropagatesUnchanged() {
    var bus = bus(deadLetters);

    Throwable thrown =
        catchThrowable(() -> bus.execute(new Adjust("c-6", 0), IdempotencyKey.of("op-6")));

    assertThat(thrown).isExactlyInstanceOf(DomainException.class);
    assertThat(deadLetters.all()).isEmpty();
  }

  @Test
  void aBusWithoutADeadLetterQueue_propagatesTheFailureItself() {
    var bus = bus(null);

    Throwable thrown = catchThrowable(() -> bus.execute(new Adjust("c-7", 5)));

    assertThat(thrown).isSameAs(storeFailure.get());
  }

  @Test
  void theReplayOfAnEntry_propagatesTheFailureItself_andStoresNoSecondEntry() {
    var bus = bus(deadLetters);

    Throwable thrown =
        catchThrowable(
            () ->
                ScopedValue.where(VirtualThreadCommandBus.DLQ_REPLAY, Boolean.TRUE)
                    .call(() -> bus.execute(new Adjust("c-8", 5), IdempotencyKey.of("op-8"))));

    assertThat(thrown).isSameAs(storeFailure.get());
    assertThat(deadLetters.all()).isEmpty();
  }

  @Test
  void aCommandASagaDispatches_isNotDeadLettered_andPropagatesTheFailureItself() {
    var bus = bus(deadLetters);

    Throwable thrown =
        catchThrowable(
            () ->
                SagaCommandDispatch.dispatch(
                    bus,
                    SagaOrigins.of("saga-1"),
                    null,
                    new Adjust("c-9", 5),
                    IdempotencyKey.of("saga:saga-1:9:0"),
                    false));

    assertThat(thrown).isSameAs(storeFailure.get());
    assertThat(deadLetters.all()).isEmpty();
  }

  @Test
  void theConstructorRequiresTheIdsAndTheCause() {
    var command = new Adjust("c-10", 1);
    var commandId = CommandId.of("cmd-10");
    var streamId = StreamId.of(COUNTER, AggregateId.of("c-10"));
    var cause = new EventStoreException("down");

    assertThatThrownBy(
            () -> new CommandDeadLetteredException(null, commandId, streamId, null, cause))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new CommandDeadLetteredException(command, null, streamId, null, cause))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(
            () -> new CommandDeadLetteredException(command, commandId, null, null, cause))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(
            () -> new CommandDeadLetteredException(command, commandId, streamId, null, null))
        .isInstanceOf(NullPointerException.class);
    assertThat(new CommandDeadLetteredException(command, commandId, streamId, null, cause))
        .isInstanceOf(CommandExecutionException.class)
        .hasCause(cause);
  }

  // ---- fixtures ----------------------------------------------------------------------------

  private VirtualThreadCommandBus bus(DeadLetterQueue deadLetterQueue) {
    var builder =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(inbox)
            .interceptors(List.of(interceptor))
            .register(COUNTER, Adjust.class, c -> AggregateId.of(c.id()), new CounterDecider());
    if (deadLetterQueue != null) {
      builder.deadLetterQueue(deadLetterQueue).objectMapper(new ObjectMapper());
    }
    return builder.build();
  }

  record Adjust(String id, int delta) implements Command {}

  record Adjusted(String id, int delta) implements DomainEvent {}

  record CounterState(int value) implements AggregateState {}

  /** Adjusts by a non-zero delta; a zero delta is a business rejection. */
  static final class CounterDecider implements Decider<Adjust, CounterState, Adjusted> {
    @Override
    public CounterState initialState() {
      return new CounterState(0);
    }

    @Override
    public List<Adjusted> decide(Adjust command, CounterState state) {
      if (command.delta() == 0) {
        throw new DomainException("a zero adjustment changes nothing");
      }
      return List.of(new Adjusted(command.id(), command.delta()));
    }

    @Override
    public CounterState evolve(CounterState state, Adjusted event) {
      return new CounterState(state.value() + event.delta());
    }
  }

  /** Records what {@code onError} receives. */
  static final class RecordingInterceptor implements CommandInterceptor {
    final List<Throwable> errors = new CopyOnWriteArrayList<>();

    @Override
    public void onError(CommandContext ctx, Throwable error) {
      errors.add(error);
    }
  }
}
