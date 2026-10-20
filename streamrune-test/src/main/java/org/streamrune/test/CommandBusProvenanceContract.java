package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.ReservedCorrelationIdException;
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
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdConstraints;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

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
 *   <li>A stored event carries a correlation id with the reserved prefix ({@link
 *       IdConstraints#RESERVED_PREFIX}) if and only if it carries a saga origin. With an origin
 *       bound the correlation id is the origin's ({@link SagaOrigin#correlationId()}), whatever
 *       ordinary id the bound request context holds. A command whose bound correlation id is
 *       reserved and is not the bound origin's — no origin is bound, or the origin of another saga
 *       — is refused with a {@link ReservedCorrelationIdException} before anything is stored.
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
    // An id of the caller's own, the bare id of a saga, and three in the reserved form: the one
    // saga dispatch binds for that saga, the prefix with something else behind it, the prefix
    // alone.
    List<String> correlationIds =
        Arrays.asList(
            UUID.randomUUID().toString(),
            SAGA_ID.value(),
            ORIGIN.correlationId().value(),
            IdConstraints.RESERVED_PREFIX + "anything",
            IdConstraints.RESERVED_PREFIX);
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
              () -> {
                List<EventEnvelope> stored = storedEventsOf(sagaOwnedCarrier(null, sagaOwned));
                assertNoProvenance(stored);
                for (EventEnvelope envelope : stored) {
                  assertFalse(
                      IdConstraints.isReserved(envelope.metadata().correlationId().value()),
                      "a generated correlation id is not in the reserved form");
                }
              }));
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
                      () -> {
                        ScopedValue.Carrier bindings =
                            sagaOwnedCarrier(
                                ScopedValue.where(StreamRuneContext.CURRENT, context), sagaOwned);
                        if (IdConstraints.isReserved(correlationId)) {
                          assertRefusedAndNothingStored(bindings);
                        } else {
                          List<EventEnvelope> stored = storedEventsOf(bindings);
                          assertNoProvenance(stored);
                          for (EventEnvelope envelope : stored) {
                            assertEquals(
                                CorrelationId.of(correlationId),
                                envelope.metadata().correlationId());
                          }
                        }
                      }));
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
                  List<EventEnvelope> stored = storedEventsOf(causedBy(cause, context));
                  assertEquals(2, stored.size());
                  for (EventEnvelope envelope : stored) {
                    assertEquals(cause.sagaOrigin(), envelope.metadata().sagaOrigin());
                    assertEquals(cause.causationId(), envelope.metadata().causationId());
                    assertEquals(
                        cause.sagaOrigin() != null,
                        IdConstraints.isReserved(envelope.metadata().correlationId().value()),
                        "a reserved correlation id, if and only if an origin");
                  }
                }));
      }
    }
    return tests.stream();
  }

  /**
   * With a saga origin bound, the correlation id of every stored event is the origin's, in the
   * reserved form — derived from the origin, not copied from the bound request context, which here
   * holds the origin's id, an ordinary id of its own, or is absent.
   */
  @TestFactory
  public Stream<DynamicTest> withAnOriginBound_theCorrelationIdIsDerivedFromIt() {
    List<StreamRuneContext.DispatchCause> causes =
        List.of(
            new StreamRuneContext.DispatchCause(CausationId.of("evt-1"), ORIGIN),
            new StreamRuneContext.DispatchCause(null, ORIGIN));
    List<String> boundCorrelationIds =
        Arrays.asList(
            null, ORIGIN.correlationId().value(), UUID.randomUUID().toString(), SAGA_ID.value());

    List<DynamicTest> tests = new ArrayList<>();
    for (StreamRuneContext.DispatchCause cause : causes) {
      for (String bound : boundCorrelationIds) {
        StreamRuneContext.RequestContext context =
            bound == null
                ? null
                : new StreamRuneContext.RequestContext(
                    null, null, CorrelationId.of(bound), Instant.now(), Map.of());
        tests.add(
            dynamicTest(
                "cause=" + cause + ", bound correlation id " + bound,
                () -> {
                  List<EventEnvelope> stored = storedEventsOf(causedBy(cause, context));
                  assertEquals(2, stored.size());
                  for (EventEnvelope envelope : stored) {
                    assertEquals(
                        CorrelationId.of("streamrune:saga:order-saga-42"),
                        envelope.metadata().correlationId());
                    assertEquals(
                        envelope.metadata().sagaOrigin().correlationId(),
                        envelope.metadata().correlationId());
                  }
                }));
      }
    }
    return tests.stream();
  }

  /**
   * A reserved correlation id beside the origin of another saga: the id of another saga, or a
   * reserved id that is no saga's. Saga dispatch and a continuation bind an origin and that
   * origin's own id, so this was bound by hand. The command is refused before the bus stores
   * anything; executed, its events would carry one saga's origin under a context, and a command
   * audit row, that name another.
   */
  @TestFactory
  public Stream<DynamicTest>
      aReservedCorrelationIdThatIsNotTheBoundOrigins_isRefusedBeforeAnythingIsStored() {
    List<String> foreign =
        List.of(
            IdConstraints.RESERVED_PREFIX + "saga:another-saga",
            IdConstraints.RESERVED_PREFIX + "saga:" + SAGA_ID.value() + "0",
            IdConstraints.RESERVED_PREFIX + "anything",
            IdConstraints.RESERVED_PREFIX);
    List<StreamRuneContext.DispatchCause> causes =
        List.of(
            new StreamRuneContext.DispatchCause(CausationId.of("evt-1"), ORIGIN),
            new StreamRuneContext.DispatchCause(null, ORIGIN));

    List<DynamicTest> tests = new ArrayList<>();
    for (String correlationId : foreign) {
      for (StreamRuneContext.DispatchCause cause : causes) {
        for (Boolean sagaOwned : Arrays.asList(null, Boolean.TRUE)) {
          StreamRuneContext.RequestContext context =
              new StreamRuneContext.RequestContext(
                  null, null, CorrelationId.of(correlationId), Instant.now(), Map.of());
          tests.add(
              dynamicTest(
                  "correlationId="
                      + correlationId
                      + ", cause="
                      + cause
                      + ", SAGA_OWNED="
                      + sagaOwned,
                  () ->
                      assertRefusedAndNothingStored(
                          sagaOwnedCarrier(causedBy(cause, context), sagaOwned))));
        }
      }
    }
    return tests.stream();
  }

  /**
   * A task wrapped inside a saga's dispatch and run on another thread carries the context and the
   * cause together ({@link StreamRuneContext#wrap(Runnable)}), so the command it executes there is
   * the saga's work like one executed on the dispatching thread: not refused, and its events carry
   * the origin and the origin's correlation id.
   */
  @Test
  public void aTaskWrappedInsideASagaDispatch_executesItsCommandAsTheSagasWork() throws Exception {
    InMemoryEventStore eventStore = new InMemoryEventStore();
    CommandBus bus = newBus(eventStore);
    try {
      StreamRuneContext.RequestContext dispatched =
          new StreamRuneContext.RequestContext(
              null, null, ORIGIN.correlationId(), Instant.now(), Map.of());
      Runnable command = () -> bus.execute(new Mark("wrapped-1"));
      Runnable task =
          sagaOwnedCarrier(
                  causedBy(
                      new StreamRuneContext.DispatchCause(CausationId.of("evt-1"), ORIGIN),
                      dispatched),
                  Boolean.TRUE)
              .call(() -> StreamRuneContext.wrap(command));

      Throwable[] failure = new Throwable[1];
      Thread other = new Thread(task);
      other.setUncaughtExceptionHandler((thread, thrown) -> failure[0] = thrown);
      other.start();
      other.join();

      assertNull(failure[0], "the wrapped command is not refused");
      List<EventEnvelope> stored =
          eventStore.load(StreamId.of(AGGREGATE_TYPE, AggregateId.of("wrapped-1"))).events();
      assertEquals(2, stored.size());
      for (EventEnvelope envelope : stored) {
        assertEquals(ORIGIN, envelope.metadata().sagaOrigin());
        assertEquals(ORIGIN.correlationId(), envelope.metadata().correlationId());
        assertEquals(CausationId.of("evt-1"), envelope.metadata().causationId());
      }
    } finally {
      close(bus);
    }
  }

  /**
   * {@code ScopedValue.where(CURRENT, StreamRuneContext.capture())} on a thread with nothing bound
   * binds the context to {@code null}. A bus takes such a binding for no context: it executes the
   * command as it would with nothing bound — no user, a correlation id of its own that is not
   * reserved, no saga origin — and does not refuse it with {@link ReservedCorrelationIdException}.
   */
  @Test
  public void aContextBoundToNull_isNoContext() {
    InMemoryEventStore eventStore = new InMemoryEventStore();
    CommandBus bus = newBus(eventStore);
    try {
      ScopedValue.where(StreamRuneContext.CURRENT, (StreamRuneContext.RequestContext) null)
          .run(() -> bus.execute(new Mark(AGGREGATE_ID)));

      List<EventEnvelope> stored = eventStore.readGlobalStream(GlobalOffset.initial(), 10);
      assertEquals(2, stored.size(), "the command executes");
      for (EventEnvelope envelope : stored) {
        assertNull(envelope.metadata().userId());
        assertNull(envelope.metadata().sagaOrigin());
        assertNotNull(envelope.metadata().correlationId());
        assertFalse(IdConstraints.isReserved(envelope.metadata().correlationId().value()));
      }
    } finally {
      close(bus);
    }
  }

  /**
   * A correlation id in the reserved form, bound by something that is neither a saga dispatch nor a
   * continuation of a saga-caused event: a context built with the plain constructor from a value a
   * request supplied, or a correlation id copied by hand from a saga-caused event. The command is
   * refused before the bus stores anything, whatever else is bound — the saga flag, a cause that
   * names a causing event and no origin.
   */
  @TestFactory
  public Stream<DynamicTest>
      aReservedCorrelationIdWithoutAnOrigin_isRefusedBeforeAnythingIsStored() {
    List<String> reserved =
        List.of(
            IdConstraints.RESERVED_PREFIX + "saga:victim",
            ORIGIN.correlationId().value(),
            IdConstraints.RESERVED_PREFIX + "anything",
            IdConstraints.RESERVED_PREFIX);
    List<Boolean> sagaOwnedBindings = Arrays.asList(null, Boolean.TRUE, Boolean.FALSE);
    List<StreamRuneContext.DispatchCause> causes =
        Arrays.asList(null, new StreamRuneContext.DispatchCause(CausationId.of("evt-1"), null));

    List<DynamicTest> tests = new ArrayList<>();
    for (String correlationId : reserved) {
      for (Boolean sagaOwned : sagaOwnedBindings) {
        for (StreamRuneContext.DispatchCause cause : causes) {
          StreamRuneContext.RequestContext context =
              new StreamRuneContext.RequestContext(
                  null,
                  UserId.of("u-1"),
                  CorrelationId.of(correlationId),
                  Instant.now(),
                  Map.of("sagaOrigin", "x"),
                  new UserAuthority(Set.of("ADMIN"), Set.of()));
          ScopedValue.Carrier bindings =
              sagaOwnedCarrier(
                  cause == null
                      ? ScopedValue.where(StreamRuneContext.CURRENT, context)
                      : causedBy(cause, context),
                  sagaOwned);
          tests.add(
              dynamicTest(
                  "correlationId="
                      + correlationId
                      + ", SAGA_OWNED="
                      + sagaOwned
                      + ", cause="
                      + (cause == null ? "none" : "a causing event, no origin"),
                  () -> assertRefusedAndNothingStored(bindings)));
        }
      }
    }
    return tests.stream();
  }

  /**
   * {@link StreamRuneContext#continuing(EventEnvelope)} trusts the envelope it is given. For an
   * event that holds a reserved correlation id and no saga origin — metadata no bus writes; an
   * event appended to the store by other means can hold it — the continuation binds the reserved id
   * and no origin, and the bus refuses its command.
   */
  @Test
  public void continuingAnEventWithAReservedCorrelationIdAndNoOrigin_isRefused() {
    EventEnvelope appendedByHand =
        new EventEnvelope(
            GlobalOffset.of(7),
            StreamId.of(AGGREGATE_TYPE, AggregateId.of("appended-by-hand")),
            Version.initial(),
            EventType.fromClass(Marked.class),
            new Marked("appended-by-hand", 1),
            new EventMetadata(
                IdGenerator.generateEventId(),
                IdGenerator.generateCommandId(),
                null,
                null,
                ORIGIN.correlationId(),
                null,
                null,
                Instant.now()));

    assertRefusedAndNothingStored(StreamRuneContext.continuing(appendedByHand));
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
                  bus, eventStore, "reply-1", causedBy(sagaDispatch, contextWithBaggage(Map.of())))
              .getFirst();
      assertEquals(0, reply.metadata().sagaOrigin().hop());
      assertEquals(ORIGIN.correlationId(), reply.metadata().correlationId());

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
    assertEquals(
        CorrelationId.of("streamrune:saga:order-saga-42"),
        continued.metadata().correlationId(),
        "every hop of a saga's work keeps the saga's reserved correlation id");
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

  /**
   * The bindings of a dispatch with {@code cause} bound, under {@code context} when one is given.
   * The one place the contract binds a cause.
   */
  private static ScopedValue.Carrier causedBy(
      StreamRuneContext.DispatchCause cause, StreamRuneContext.RequestContext context) {
    ScopedValue.Carrier bindings = ScopedValue.where(StreamRuneContext.CAUSE, cause);
    return context == null ? bindings : bindings.where(StreamRuneContext.CURRENT, context);
  }

  /**
   * One {@link Mark} executed on a fresh bus under {@code bindings} is refused for its reserved
   * correlation id, and the event store holds no event afterwards, on any stream.
   */
  private void assertRefusedAndNothingStored(ScopedValue.Carrier bindings) {
    InMemoryEventStore eventStore = new InMemoryEventStore();
    CommandBus bus = newBus(eventStore);
    try {
      ReservedCorrelationIdException refused =
          assertThrows(
              ReservedCorrelationIdException.class,
              () -> bindings.run(() -> bus.execute(new Mark(AGGREGATE_ID))));
      assertEquals(
          "the bound correlation id uses the reserved 'streamrune:' prefix and is not the bound saga"
              + " origin's; only a saga dispatch or StreamRuneContext.continuing binds it",
          refused.getMessage());
      assertEquals(List.of(), eventStore.readGlobalStream(GlobalOffset.initial(), 10));
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

  /**
   * A request context that carries {@code baggage} and, as its correlation id, the bare id of the
   * contract's saga: not the reserved one. The cases that bind it beside an origin therefore also
   * show that the stored correlation id is the origin's and not this one.
   */
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
