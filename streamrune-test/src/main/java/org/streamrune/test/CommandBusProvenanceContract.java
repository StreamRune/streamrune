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
 * The provenance contract every {@link CommandBus} must honour:
 *
 * <ul>
 *   <li>The saga origin and the causation id of a produced event come from the bound {@link
 *       StreamRuneContext#CAUSE} and from nothing else. With no cause bound no event carries an
 *       origin or a causation id, whatever the correlation id, the baggage, the user, the captured
 *       authority or the saga flag.
 *   <li>A bound cause decides both for every produced event, a {@code null} part included, whatever
 *       the baggage holds.
 *   <li>A command executed inside {@link StreamRuneContext#continuing(EventEnvelope)} stores what
 *       that helper binds: the causing event's id as the causation id, its saga origin one hop
 *       further (or none), its correlation and trace ids, and no user or baggage.
 * </ul>
 *
 * <p>Run by {@code InMemoryCommandBusProvenanceTest} and {@code
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
  private record ContractSagaState(SagaStatus status) implements SagaState {}

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

  /** A baggage key a bus gives no meaning; an entry under it never becomes a causation id. */
  private static final String CAUSATION_BAGGAGE_KEY = "streamrune.causation-id";

  private static final SagaId SAGA_ID = SagaId.of("order-saga-42");

  /** The origin the contract binds; built as saga dispatch builds it. */
  protected static final SagaOrigin ORIGIN =
      SagaEnvelopes.metadataCausedBy(ContractSagaState.class, SAGA_ID).sagaOrigin();

  @TestFactory
  public Stream<DynamicTest> withNoCauseBound_noEventCarriesAnOriginOrACausationId() {
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
              () -> assertNoProvenance(storedEventsOf(sagaOwnedCarrier(null, sagaOwned)))));
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
                          assertNoProvenance(
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
   * A listener's hop, and the hop after it: each command runs inside {@link
   * StreamRuneContext#continuing(EventEnvelope)} of an event the previous one stored. Run once with
   * nothing else bound and once on a thread that holds a user's request context and the saga flag,
   * which a continuation must not pass on.
   */
  @TestFactory
  public Stream<DynamicTest> continuing_storesOriginHopAndCausation() {
    StreamRuneContext.RequestContext caller =
        new StreamRuneContext.RequestContext(
            null,
            UserId.of("caller"),
            CorrelationId.of("caller-corr"),
            Instant.now(),
            Map.of("role", "ADMIN", CAUSATION_BAGGAGE_KEY, "evt-9"),
            new UserAuthority(Set.of("ADMIN"), Set.of()));
    ScopedValue.Carrier sagaThread =
        ScopedValue.where(StreamRuneContext.CURRENT, caller)
            .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE);
    return Stream.of(
        dynamicTest(
            "an event a saga caused, nothing else bound",
            () -> assertContinuationsCarryTheOrigin(null)),
        dynamicTest(
            "an event a saga caused, on a thread with a user and the saga flag",
            () -> assertContinuationsCarryTheOrigin(sagaThread)),
        dynamicTest(
            "an event no saga caused, nothing else bound",
            () -> assertAContinuationOfAnOriginlessEventCarriesNone(null)),
        dynamicTest(
            "an event no saga caused, on a thread with a user and the saga flag",
            () -> assertAContinuationOfAnOriginlessEventCarriesNone(sagaThread)));
  }

  private void assertContinuationsCarryTheOrigin(ScopedValue.Carrier ambient) {
    InMemoryEventStore eventStore = new InMemoryEventStore();
    CommandBus bus = newBus(eventStore);
    try {
      // What a saga's own command stored: the origin at hop 0.
      StreamRuneContext.DispatchCause sagaDispatch =
          new StreamRuneContext.DispatchCause(CausationId.of("evt-trigger"), ORIGIN);
      EventEnvelope reply =
          executeAndLoad(
                  bus,
                  eventStore,
                  "reply-1",
                  ScopedValue.where(StreamRuneContext.CAUSE, sagaDispatch)
                      .where(StreamRuneContext.CURRENT, contextWithBaggage(Map.of())))
              .getFirst();
      assertEquals(0, reply.metadata().sagaOrigin().hop());

      List<EventEnvelope> firstHop = continueAndLoad(bus, eventStore, "hop-1", ambient, reply);
      assertEquals(2, firstHop.size());
      for (EventEnvelope envelope : firstHop) {
        assertContinues(reply, envelope, 1);
      }

      List<EventEnvelope> secondHop =
          continueAndLoad(bus, eventStore, "hop-2", ambient, firstHop.getLast());
      assertEquals(2, secondHop.size());
      for (EventEnvelope envelope : secondHop) {
        assertContinues(firstHop.getLast(), envelope, 2);
      }
    } finally {
      close(bus);
    }
  }

  private void assertAContinuationOfAnOriginlessEventCarriesNone(ScopedValue.Carrier ambient) {
    InMemoryEventStore eventStore = new InMemoryEventStore();
    CommandBus bus = newBus(eventStore);
    try {
      // What a user's request stored: a correlation id of the client's choosing, a user, baggage.
      StreamRuneContext.RequestContext request =
          new StreamRuneContext.RequestContext(
              null,
              UserId.of("alice"),
              CorrelationId.of("client-corr-7"),
              Instant.now(),
              Map.of("role", "ADMIN"),
              new UserAuthority(Set.of("ADMIN"), Set.of()));
      EventEnvelope requested =
          executeAndLoad(
                  bus,
                  eventStore,
                  "request-1",
                  ScopedValue.where(StreamRuneContext.CURRENT, request))
              .getFirst();
      assertEquals(UserId.of("alice"), requested.metadata().userId());

      List<EventEnvelope> continued = continueAndLoad(bus, eventStore, "hop-1", ambient, requested);
      assertEquals(2, continued.size());
      for (EventEnvelope envelope : continued) {
        assertNull(envelope.metadata().sagaOrigin());
        assertEquals(
            CausationId.of(requested.metadata().eventId().value()),
            envelope.metadata().causationId());
        assertEquals(CorrelationId.of("client-corr-7"), envelope.metadata().correlationId());
        assertNull(envelope.metadata().userId());
        assertEquals(Map.of(), envelope.metadata().baggage());
      }
    } finally {
      close(bus);
    }
  }

  /** The stored event {@code continued} continues {@code cause} at {@code hop}. */
  private static void assertContinues(EventEnvelope cause, EventEnvelope continued, int hop) {
    SagaOrigin origin = continued.metadata().sagaOrigin();
    assertEquals(ORIGIN.sagaType(), origin.sagaType());
    assertEquals(ORIGIN.sagaId(), origin.sagaId());
    assertEquals(hop, origin.hop());
    assertEquals(
        CausationId.of(cause.metadata().eventId().value()), continued.metadata().causationId());
    assertEquals(cause.metadata().correlationId(), continued.metadata().correlationId());
    assertEquals(cause.metadata().traceId(), continued.metadata().traceId());
    assertNull(continued.metadata().userId());
    assertEquals(Map.of(), continued.metadata().baggage());
  }

  /**
   * Executes one {@link Mark} as a listener does that continues {@code cause} on a thread that
   * already holds {@code ambient} ({@code null}: holds nothing), and returns the stored events.
   */
  private static List<EventEnvelope> continueAndLoad(
      CommandBus bus,
      InMemoryEventStore eventStore,
      String aggregateId,
      ScopedValue.Carrier ambient,
      EventEnvelope cause) {
    Runnable listener =
        () -> StreamRuneContext.continuing(cause).run(() -> bus.execute(new Mark(aggregateId)));
    if (ambient == null) {
      listener.run();
    } else {
      ambient.run(listener);
    }
    return eventStore.load(StreamId.of(AGGREGATE_TYPE, AggregateId.of(aggregateId))).events();
  }

  private static List<EventEnvelope> executeAndLoad(
      CommandBus bus, InMemoryEventStore eventStore, String aggregateId, ScopedValue.Carrier in) {
    in.run(() -> bus.execute(new Mark(aggregateId)));
    return eventStore.load(StreamId.of(AGGREGATE_TYPE, AggregateId.of(aggregateId))).events();
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

  private static void assertNoProvenance(List<EventEnvelope> stored) {
    assertEquals(2, stored.size());
    for (EventEnvelope envelope : stored) {
      assertNull(envelope.metadata().sagaOrigin());
      assertNull(envelope.metadata().causationId());
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
