package org.streamrune.test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DeciderRegistrations;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.NoDeciderException;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * In-memory {@link CommandBus} implementation for unit testing.
 *
 * <p>This implementation is intentionally minimal: no locking, no retries, no interceptors, no
 * metrics. It dispatches commands via the Decider pattern, reconstructing aggregate state from the
 * {@link EventStore} before each command, running the decider's {@link Decider#guard authorization
 * hook} exactly as {@code VirtualThreadCommandBus} does (a guard skipped here made authz tests
 * false-green), and appending resulting events back to the store.
 *
 * <p>Event metadata follows the same topology as {@code VirtualThreadCommandBus}: all events of one
 * command share a single correlation id (taken from {@link StreamRuneContext} when bound, generated
 * otherwise), the causation id comes from the {@link #CAUSATION_ID_BAGGAGE_KEY} baggage entry, and
 * envelope versions match what the store persists.
 *
 * <p><strong>Note: This class is not thread-safe.</strong> Do not share a single instance across
 * threads. Use a separate {@link InMemoryCommandBus} per test thread.
 */
public final class InMemoryCommandBus implements CommandBus, AutoCloseable {

  /**
   * Baggage key carrying the id of the message that caused the current command. Mirrors {@code
   * VirtualThreadCommandBus.CAUSATION_ID_BAGGAGE_KEY} (streamrune-runtime, which this module does
   * not depend on) — the two values must stay identical.
   */
  public static final String CAUSATION_ID_BAGGAGE_KEY = "streamrune.causation-id";

  /**
   * Binds a command type to its decider, its aggregate type and its aggregate ID extractor.
   *
   * @param aggregateType the aggregate type this registration's streams belong to
   * @param commandType the command class
   * @param idExtractor extracts the aggregate ID from a command instance
   * @param decider the decider that handles commands of this type
   * @param <C> command type
   * @param <S> state type
   * @param <E> event type
   */
  public record DeciderRegistration<
      C extends Command, S extends AggregateState, E extends DomainEvent>(
      AggregateType aggregateType,
      Class<C> commandType,
      Function<C, AggregateId> idExtractor,
      Decider<C, S, E> decider) {

    public DeciderRegistration {
      if (aggregateType == null) {
        throw new IllegalArgumentException("aggregateType is required");
      }
      if (commandType == null) {
        throw new IllegalArgumentException("commandType is required");
      }
      if (idExtractor == null) {
        throw new IllegalArgumentException("idExtractor is required");
      }
      if (decider == null) {
        throw new IllegalArgumentException("decider is required");
      }
    }
  }

  private final EventStore eventStore;
  private final Map<Class<?>, Decider<?, ?, ?>> deciders = new HashMap<>();
  private final Map<Class<?>, Function<?, AggregateId>> idExtractors = new HashMap<>();
  private final Map<Class<?>, AggregateType> aggregateTypes = new HashMap<>();
  private final List<DeciderRegistrations.Registration> accepted = new ArrayList<>();
  private boolean closed = false;

  private InMemoryCommandBus(EventStore eventStore) {
    this.eventStore = eventStore;
  }

  /** Creates a new builder for {@link InMemoryCommandBus}. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Registers a decider for the given registration, under the same rule every command bus applies:
   * a command type routes to exactly one decider, overlapping command roots declare one aggregate
   * type, and one decider instance belongs to one aggregate type.
   *
   * @param registration the decider registration
   * @param <C> command type
   * @param <S> state type
   * @param <E> event type
   * @return this bus for chaining
   * @throws IllegalArgumentException if the registration breaks the rule
   */
  public <C extends Command, S extends AggregateState, E extends DomainEvent>
      InMemoryCommandBus register(DeciderRegistration<C, S, E> registration) {
    var rule = ruleOf(registration);
    DeciderRegistrations.requireCompatible(accepted, rule);
    accepted.add(rule);
    deciders.put(registration.commandType(), registration.decider());
    idExtractors.put(registration.commandType(), registration.idExtractor());
    aggregateTypes.put(registration.commandType(), registration.aggregateType());
    return this;
  }

  private static DeciderRegistrations.Registration ruleOf(DeciderRegistration<?, ?, ?> r) {
    return new DeciderRegistrations.Registration(r.aggregateType(), r.commandType(), r.decider());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <C extends Command> CommandResult execute(C command) {
    if (closed) {
      throw new IllegalStateException("CommandBus has been closed");
    }

    Class<?> commandType = command.getClass();
    Decider<C, ?, DomainEvent> decider = (Decider<C, ?, DomainEvent>) deciders.get(commandType);
    if (decider == null) {
      throw new NoDeciderException(commandType, new ArrayList<>(deciders.keySet()));
    }

    Function<C, AggregateId> idExtractor = (Function<C, AggregateId>) idExtractors.get(commandType);

    AggregateId aggregateId = idExtractor.apply(command);
    if (aggregateId == null) {
      throw new IllegalArgumentException(
          "idExtractor returned null for command: " + command.getClass().getSimpleName());
    }
    StreamId streamId = StreamId.of(aggregateTypes.get(commandType), aggregateId);

    // Reconstruct current state from event store
    AggregateHistory history = eventStore.load(streamId);
    AggregateState state = history.snapshotState();

    @SuppressWarnings("unchecked")
    Decider<C, AggregateState, DomainEvent> anyDecider =
        (Decider<C, AggregateState, DomainEvent>) decider;

    if (state == null) {
      state = anyDecider.initialState();
    }

    // Evolve state through existing events
    for (EventEnvelope envelope : history.events()) {
      state = evolve(state, envelope.event(), anyDecider);
    }

    // The authorization hook runs exactly where the production bus runs it — after
    // the state is reconstructed and BEFORE decide. The published test-support bus used to skip it
    // entirely, so a user's authz test (a guard that must reject a foreign owner) passed
    // false-green against this bus and only failed in production. A throwing guard propagates
    // as-is, exactly like VirtualThreadCommandBus: nothing is decided, nothing is appended.
    anyDecider.guard(command, state);

    // Decide
    List<? extends DomainEvent> events = anyDecider.decide(command, state);

    if (events.isEmpty()) {
      return new CommandResult(List.of(), streamId, history.version(), List.of());
    }

    // Wrap events as envelopes. Versions are expectedVersion+1..+n and the correlation id is
    // shared by all events of this command, mirroring VirtualThreadCommandBus and what the
    // store persists.
    CommandId commandId = IdGenerator.generateCommandId();
    CorrelationId correlationId = correlationId();
    List<EventEnvelope> envelopes = new ArrayList<>();
    long version = history.version().value();
    for (DomainEvent event : events) {
      version++;
      envelopes.add(
          new EventEnvelope(
              GlobalOffset.initial(),
              streamId,
              new Version(version),
              EventType.fromClass(event.getClass()),
              event,
              buildMetadata(commandId, correlationId)));
    }

    // Append to store
    EventStore.AppendResult appendResult =
        eventStore.append(streamId, envelopes, history.version());

    // Rebuild envelopes with store-assigned global offsets for InlineProjectionInterceptor
    List<EventEnvelope> finalEnvelopes = new java.util.ArrayList<>(envelopes.size());
    for (int i = 0; i < envelopes.size(); i++) {
      var original = envelopes.get(i);
      finalEnvelopes.add(
          new EventEnvelope(
              appendResult.globalOffsets().get(i),
              original.streamId(),
              original.version(),
              original.eventType(),
              original.event(),
              original.metadata()));
    }

    return new CommandResult(
        events,
        streamId,
        appendResult.finalVersion(),
        appendResult.globalOffsets(),
        List.copyOf(finalEnvelopes));
  }

  @SuppressWarnings("unchecked")
  private <C extends Command, S extends AggregateState, E extends DomainEvent>
      AggregateState evolve(AggregateState state, DomainEvent event, Decider<C, S, E> decider) {
    return decider.evolve((S) state, (E) event);
  }

  private static CorrelationId correlationId() {
    if (StreamRuneContext.CURRENT.isBound()) {
      return StreamRuneContext.CURRENT.get().correlationId();
    }
    return IdGenerator.generateCorrelationId();
  }

  /**
   * Builds event metadata with the same topology as {@code VirtualThreadCommandBus}: one
   * correlation id per command (from {@link StreamRuneContext} when bound), causation id read from
   * the {@link #CAUSATION_ID_BAGGAGE_KEY} baggage entry (events are causation roots otherwise) —
   * never the command's own id, which lives in {@link EventMetadata#commandId()}.
   */
  private static EventMetadata buildMetadata(CommandId commandId, CorrelationId correlationId) {
    EventId eventId = IdGenerator.generateEventId();
    if (StreamRuneContext.CURRENT.isBound()) {
      StreamRuneContext.RequestContext ctx = StreamRuneContext.CURRENT.get();
      return new EventMetadata(
          eventId,
          commandId,
          ctx.traceId(),
          null,
          correlationId,
          causationIdFrom(ctx),
          ctx.userId(),
          ctx.timestamp(),
          ctx.baggage());
    }
    return new EventMetadata(
        eventId, commandId, null, null, correlationId, null, null, Instant.now());
  }

  private static CausationId causationIdFrom(StreamRuneContext.RequestContext ctx) {
    String causingMessageId = ctx.baggage().get(CAUSATION_ID_BAGGAGE_KEY);
    if (causingMessageId == null || causingMessageId.isBlank()) {
      return null;
    }
    return CausationId.of(causingMessageId);
  }

  @Override
  public void close() {
    closed = true;
  }

  /** Builder for {@link InMemoryCommandBus}. */
  public static final class Builder {

    private EventStore eventStore;
    private final List<DeciderRegistration<?, ?, ?>> registrations = new ArrayList<>();
    private final List<DeciderRegistrations.Registration> accepted = new ArrayList<>();

    private Builder() {}

    /** Sets the event store. Required. */
    public Builder eventStore(EventStore eventStore) {
      this.eventStore = eventStore;
      return this;
    }

    /**
     * Registers a decider. The registration rule runs here, at the offending call, exactly as on
     * the production bus: a command type routes to exactly one decider, overlapping command roots
     * declare one aggregate type, and one decider instance belongs to one aggregate type.
     *
     * @param registration the decider registration
     * @param <C> command type
     * @param <S> state type
     * @param <E> event type
     * @return this builder
     * @throws IllegalArgumentException if the registration breaks the rule
     */
    public <C extends Command, S extends AggregateState, E extends DomainEvent> Builder register(
        DeciderRegistration<C, S, E> registration) {
      var rule = ruleOf(registration);
      DeciderRegistrations.requireCompatible(accepted, rule);
      accepted.add(rule);
      registrations.add(registration);
      return this;
    }

    /** Builds the {@link InMemoryCommandBus}. */
    @SuppressWarnings("unchecked")
    public InMemoryCommandBus build() {
      if (eventStore == null) {
        throw new IllegalArgumentException("eventStore is required");
      }
      InMemoryCommandBus bus = new InMemoryCommandBus(eventStore);
      for (DeciderRegistration<?, ?, ?> reg : registrations) {
        bus.register((DeciderRegistration) reg);
      }
      return bus;
    }
  }
}
