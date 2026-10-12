package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;

/**
 * The provenance contract every {@link CommandBus} must honour: the saga origin and the causation
 * id of a produced event come from the bound {@link StreamRuneContext#CAUSE} and from nothing else
 * a caller controls. Run by {@code InMemoryCommandBusProvenanceTest} and {@code
 * VirtualThreadCommandBusProvenanceTest}, so the test bus and the production bus cannot diverge
 * unnoticed.
 *
 * <p>A subclass supplies the bus under test; the contract executes one command per case against a
 * fresh {@link InMemoryEventStore} and reads the stored events back.
 */
public abstract class CommandBusProvenanceContract {

  /** The contract's command: stores two {@link Marked} events on the aggregate it names. */
  public record Mark(String aggregateId) implements Command {}

  /** The contract's event. */
  public record Marked(String aggregateId, int ordinal) implements DomainEvent {}

  /** The contract's aggregate state. */
  public record Marks(int count) implements AggregateState {}

  /** The state type the contract's saga origin names. */
  public record ContractSagaState(SagaStatus status) implements SagaState {}

  /** The aggregate type the contract's decider is registered under. */
  public static final AggregateType AGGREGATE_TYPE = AggregateType.of("provenance_contract");

  /** The contract's one decider: every {@link Mark} produces two {@link Marked} events. */
  public static final Decider<Mark, Marks, Marked> DECIDER =
      new Decider<>() {
        @Override
        public Marks initialState() {
          return new Marks(0);
        }

        @Override
        public List<Marked> decide(Mark command, Marks state) {
          return List.of(
              new Marked(command.aggregateId(), state.count() + 1),
              new Marked(command.aggregateId(), state.count() + 2));
        }

        @Override
        public Marks evolve(Marks state, Marked event) {
          return new Marks(state.count() + 1);
        }
      };

  /**
   * The aggregate id of a {@link Mark} command; the id extractor a subclass registers.
   *
   * @param command the contract's command
   * @return the aggregate the command addresses
   */
  public static AggregateId aggregateIdOf(Mark command) {
    return AggregateId.of(command.aggregateId());
  }

  /**
   * A bus over {@code eventStore} with {@link #DECIDER} registered for {@link Mark} under {@link
   * #AGGREGATE_TYPE} and {@link #aggregateIdOf(Mark)}, and nothing else that could refuse or alter
   * a command (no interceptors).
   *
   * @param eventStore the store the bus appends to
   * @return the bus under test; closed by the contract when it is {@link AutoCloseable}
   */
  protected abstract CommandBus newBus(EventStore eventStore);

  private static final String AGGREGATE_ID = "marked-1";
  private static final String CAUSATION_BAGGAGE_KEY = "streamrune.causation-id";
  private static final SagaId SAGA_ID = SagaId.of("order-saga-42");

  /** The origin the contract binds; built as saga dispatch builds it. */
  protected static final SagaOrigin ORIGIN =
      SagaEnvelopes.metadataCausedBy(ContractSagaState.class, SAGA_ID).sagaOrigin();

  @TestFactory
  public Stream<DynamicTest> withNoCauseBound_noEventCarriesAnOrigin() {
    List<String> correlationIds = Arrays.asList(UUID.randomUUID().toString(), SAGA_ID.value());
    List<Map<String, String>> baggages =
        List.of(Map.of(), Map.of("sagaOrigin", "x"), Map.of(CAUSATION_BAGGAGE_KEY, "evt-9"));
    List<UserId> users = Arrays.asList(null, UserId.of("u-1"));
    List<UserAuthority> authorities =
        Arrays.asList(null, new UserAuthority(Set.of("ADMIN"), Set.of()));
    List<Boolean> sagaOwnedBindings = Arrays.asList(null, Boolean.TRUE, Boolean.FALSE);

    List<DynamicTest> tests = new ArrayList<>();
    for (Boolean sagaOwned : sagaOwnedBindings) {
      tests.add(
          dynamicTest(
              "no context, SAGA_OWNED=" + sagaOwned,
              () -> assertNoOrigin(storedEventsOf(sagaOwnedCarrier(null, sagaOwned)))));
      for (String correlationId : correlationIds) {
        for (Map<String, String> baggage : baggages) {
          for (UserId user : users) {
            for (UserAuthority authority : authorities) {
              StreamRuneContext.RequestContext context =
                  new StreamRuneContext.RequestContext(
                      null,
                      user,
                      CorrelationId.of(correlationId),
                      Instant.now(),
                      baggage,
                      authority);
              tests.add(
                  dynamicTest(
                      "correlationId="
                          + correlationId
                          + ", baggage="
                          + baggage
                          + ", user="
                          + user
                          + ", authority="
                          + (authority == null ? "none" : "one role")
                          + ", SAGA_OWNED="
                          + sagaOwned,
                      () ->
                          assertNoOrigin(
                              storedEventsOf(
                                  sagaOwnedCarrier(
                                      ScopedValue.where(StreamRuneContext.CURRENT, context),
                                      sagaOwned)))));
            }
          }
        }
      }
    }
    return tests.stream();
  }

  @TestFactory
  public Stream<DynamicTest> withACauseBound_everyEventCarriesItsOriginAndItsCausationId() {
    CausationId causingEvent = CausationId.of("evt-1");
    List<StreamRuneContext.DispatchCause> causes =
        List.of(
            new StreamRuneContext.DispatchCause(causingEvent, ORIGIN),
            new StreamRuneContext.DispatchCause(null, ORIGIN),
            new StreamRuneContext.DispatchCause(causingEvent, null));
    // A context without the causation baggage entry, one with it, and no context at all.
    List<StreamRuneContext.RequestContext> contexts =
        Arrays.asList(contextWithBaggage(Map.of()), contextWithBaggage(causationBaggage()), null);

    List<DynamicTest> tests = new ArrayList<>();
    for (StreamRuneContext.DispatchCause cause : causes) {
      for (StreamRuneContext.RequestContext context : contexts) {
        tests.add(
            dynamicTest(
                "cause=" + cause + ", context=" + describe(context),
                () -> {
                  ScopedValue.Carrier bindings = ScopedValue.where(StreamRuneContext.CAUSE, cause);
                  if (context != null) {
                    bindings = bindings.where(StreamRuneContext.CURRENT, context);
                  }
                  List<EventEnvelope> stored = storedEventsOf(bindings);
                  assertEquals(2, stored.size());
                  for (EventEnvelope envelope : stored) {
                    assertEquals(cause.sagaOrigin(), envelope.metadata().sagaOrigin());
                    assertEquals(cause.causationId(), envelope.metadata().causationId());
                  }
                }));
      }
    }
    return tests.stream();
  }

  /**
   * Executes one {@link Mark} on a fresh bus under {@code bindings} ({@code null}: nothing bound)
   * and returns the events the store holds for its stream.
   *
   * @param bindings the scoped values bound around the dispatch, or {@code null}
   * @return the stored events of the command, in stream order
   */
  protected final List<EventEnvelope> storedEventsOf(ScopedValue.Carrier bindings) {
    InMemoryEventStore eventStore = new InMemoryEventStore();
    CommandBus bus = newBus(eventStore);
    try {
      Mark command = new Mark(AGGREGATE_ID);
      if (bindings == null) {
        bus.execute(command);
      } else {
        bindings.run(() -> bus.execute(command));
      }
      return eventStore.load(StreamId.of(AGGREGATE_TYPE, AggregateId.of(AGGREGATE_ID))).events();
    } finally {
      close(bus);
    }
  }

  private static void assertNoOrigin(List<EventEnvelope> stored) {
    assertEquals(2, stored.size());
    for (EventEnvelope envelope : stored) {
      assertNull(envelope.metadata().sagaOrigin());
    }
  }

  private static ScopedValue.Carrier sagaOwnedCarrier(ScopedValue.Carrier base, Boolean sagaOwned) {
    if (sagaOwned == null) {
      return base;
    }
    return base == null
        ? ScopedValue.where(StreamRuneContext.SAGA_OWNED, sagaOwned)
        : base.where(StreamRuneContext.SAGA_OWNED, sagaOwned);
  }

  private static StreamRuneContext.RequestContext contextWithBaggage(Map<String, String> baggage) {
    return new StreamRuneContext.RequestContext(
        null, null, CorrelationId.of(SAGA_ID.value()), Instant.now(), baggage);
  }

  private static Map<String, String> causationBaggage() {
    return Map.of(CAUSATION_BAGGAGE_KEY, "evt-9");
  }

  private static String describe(StreamRuneContext.RequestContext context) {
    return context == null ? "none" : "baggage " + context.baggage();
  }

  private static void close(CommandBus bus) {
    if (bus instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        throw new IllegalStateException("the bus under test failed to close", e);
      }
    }
  }
}
