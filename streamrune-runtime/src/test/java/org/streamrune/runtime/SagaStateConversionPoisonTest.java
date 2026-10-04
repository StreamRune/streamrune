package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStateSerializationException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * {@code PostgresSagaStore} now raises {@link SagaStateSerializationException} instead of
 * laundering a state-conversion failure into an {@code EventStoreException} — but that only ends
 * the subscription wedge if the runtime classifies it.
 *
 * <p>A <b>deterministic</b> conversion failure (a Jackson mapping defect, a {@code
 * CryptoShreddingModule} refusal, a crypto-shredded subject) re-fails identically on every retry:
 * it must quarantine the triggering event and FAULT the saga. A <b>transient</b> one (a Vault /
 * AWS-KMS outage, a pool timeout — the very failure mode the crypto backends' fixes deliberately
 * produce) must keep propagating so the batch retries.
 */
class SagaStateConversionPoisonTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  // ---------------------------------------------------------------- domain

  record Start(String id) implements DomainEvent {}

  record Next(String id) implements DomainEvent {}

  record DoWork(String id) implements Command {}

  record FakeState(SagaStatus status, String id) implements SagaState {}

  static final class FakeOrchestrator implements SagaOrchestrator<FakeState> {
    @Override
    public Class<FakeState> stateType() {
      return FakeState.class;
    }

    @Override
    public FakeState initialState(SagaId sagaId) {
      return new FakeState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof Start;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("saga-" + ((Start) event.event()).id());
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      String corrId = event.metadata().correlationId().value();
      return corrId.startsWith("saga-") ? Optional.of(SagaId.of(corrId)) : Optional.empty();
    }

    @Override
    public FakeState evolve(FakeState state, EventEnvelope event) {
      return event.event() instanceof Start s ? new FakeState(SagaStatus.RUNNING, s.id()) : state;
    }

    @Override
    public List<SagaCommand> handle(FakeState state, EventEnvelope event) {
      return List.of(SagaCommand.of(new DoWork(state.id()), AggregateId.of(state.id())));
    }

    @Override
    public List<SagaCommand> compensate(
        FakeState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  static final class NoopCommandBus implements CommandBus {
    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("use keyed execute");
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      return new CommandResult(
          List.of(), StreamId.of(TYPE, AggregateId.of("test")), Version.initial(), List.of());
    }
  }

  /**
   * Delegating {@link SagaStore} that raises a {@link SagaStateSerializationException} with a
   * configurable cause on a chosen operation — the exact shape {@code PostgresSagaStore} now
   * produces when {@code writeValueAsString}/{@code readValue} fails.
   */
  static final class ConversionFailingSagaStore implements SagaStore {
    private final SagaStore delegate;
    Throwable failLoadWith;
    Throwable failCreateWith;
    Throwable failUpdateWith;

    /**
     * How many update/claim calls {@link #failUpdateWith} applies to. Defaults to "every call". One
     * is the realistic shape of the finding's own scenario: the FORWARD write carries the newly
     * evolved, unconvertible state, while {@code markFaulted} writes back the previously LOADED
     * state, which by construction converted fine on the way in.
     */
    int failUpdateTimes = Integer.MAX_VALUE;

    ConversionFailingSagaStore(SagaStore delegate) {
      this.delegate = delegate;
    }

    private static void raise(SagaId sagaId, String op, Throwable cause) {
      if (cause != null) {
        throw new SagaStateSerializationException(
            "Failed to " + op + " saga state: " + sagaId, sagaId, cause);
      }
    }

    private void raiseUpdate(SagaId sagaId, String op) {
      if (failUpdateWith != null && failUpdateTimes > 0) {
        failUpdateTimes--;
        raise(sagaId, op, failUpdateWith);
      }
    }

    @Override
    public void create(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      raise(sagaId, "create", failCreateWith);
      delegate.create(sagaId, sagaType, state, status, deadLetterPending);
    }

    /** The executor's create-first write raises like {@code create}. */
    @Override
    public void createGenesisPending(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      raise(sagaId, "create", failCreateWith);
      delegate.createGenesisPending(sagaId, sagaType, state, status, deadLetterPending);
    }

    /** The runner's evolve-poison create raises like {@code create}. */
    @Override
    public void createFaulted(SagaId sagaId, SagaType sagaType, SagaState initialState) {
      raise(sagaId, "create", failCreateWith);
      delegate.createFaulted(sagaId, sagaType, initialState);
    }

    /**
     * The forward CAS is {@code applyEvent} — it carries the newly evolved, unconvertible state, so
     * it raises like {@code update} (and counts against {@link #failUpdateTimes}).
     */
    @Override
    public void applyEvent(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        long expectedVersion,
        AppliedEvent applied) {
      raiseUpdate(sagaId, "update");
      delegate.applyEvent(sagaId, sagaType, state, status, expectedVersion, applied);
    }

    /** {@code markFaulted} writes NO state, so it never raises. */
    @Override
    public boolean markFaulted(SagaId sagaId, SagaType sagaType, long expectedVersion) {
      return delegate.markFaulted(sagaId, sagaType, expectedVersion);
    }

    @Override
    public void setDeadLetterPending(SagaId sagaId, SagaType sagaType, boolean pending) {
      delegate.setDeadLetterPending(sagaId, sagaType, pending);
    }

    @Override
    public void update(
        SagaId sagaId,
        SagaType sagaType,
        SagaState state,
        SagaStatus status,
        long expectedVersion) {
      raiseUpdate(sagaId, "update");
      delegate.update(sagaId, sagaType, state, status, expectedVersion);
    }

    @Override
    public void claimCompensating(
        SagaId sagaId, SagaType sagaType, SagaState state, long expectedVersion) {
      raiseUpdate(sagaId, "claim");
      delegate.claimCompensating(sagaId, sagaType, state, expectedVersion);
    }

    @Override
    public <S extends SagaState> Optional<LoadedSaga<S>> load(
        SagaId sagaId, SagaType sagaType, Class<S> stateType) {
      raise(sagaId, "load", failLoadWith);
      return delegate.load(sagaId, sagaType, stateType);
    }

    @Override
    public List<SagaId> findTimedOut(SagaType sagaType, Instant cutoff, int limit) {
      return delegate.findTimedOut(sagaType, cutoff, limit);
    }

    @Override
    public List<SagaId> findByStatus(SagaType sagaType, SagaStatus status, int limit) {
      return delegate.findByStatus(sagaType, status, limit);
    }

    @Override
    public void delete(SagaId sagaId, SagaType sagaType) {
      delegate.delete(sagaId, sagaType);
    }
  }

  // ---------------------------------------------------------------- fixture

  private InMemorySagaStore backing;
  private ConversionFailingSagaStore store;
  private InMemorySagaDeadLetterStore deadLetters;
  private SagaRunner<FakeState> runner;
  private EventListener listener;

  private static long offsetCounter = 1;

  @BeforeEach
  void setUp() {
    backing = new InMemorySagaStore();
    store = new ConversionFailingSagaStore(backing);
    deadLetters = new InMemorySagaDeadLetterStore(backing);
    runner =
        SagaRunner.<FakeState>builder()
            .orchestrator(new FakeOrchestrator())
            .sagaStore(store)
            .commandBus(new NoopCommandBus())
            .sagaDeadLetterStore(deadLetters)
            .build();
    listener = runner.asEventListener();
  }

  /** A deterministic Jackson mapping defect: the stored shape can never bind, retry or not. */
  private static Throwable deterministicMappingFailure() {
    return new IOException("Unrecognized field \"legacyField\" — the shape can never bind");
  }

  /** A transient key-store outage, surfaced exactly as the crypto engines raise it. */
  private static Throwable transientKeyStoreOutage() {
    return new CryptoOperationException(
        "Vault request failed", new IOException("Connection refused: vault:8200"));
  }

  private EventEnvelope startEnvelope(String id) {
    return envelope(new Start(id), "some-correlation");
  }

  private EventEnvelope correlatedEnvelope(String sagaId, String id) {
    return envelope(new Next(id), sagaId);
  }

  private EventEnvelope envelope(DomainEvent event, String correlationId) {
    return new EventEnvelope(
        GlobalOffset.of(offsetCounter++),
        StreamId.of(TYPE, AggregateId.of("test-stream")),
        Version.initial(),
        new EventType(event.getClass().getSimpleName()),
        event,
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-1"),
            null,
            null,
            CorrelationId.of(correlationId),
            null,
            null,
            Instant.now()));
  }

  private void seedRunningSaga(String sagaId) {
    backing.create(
        SagaId.of(sagaId),
        SagaType.fromClass(FakeState.class),
        new FakeState(SagaStatus.RUNNING, sagaId),
        SagaStatus.RUNNING);
  }

  // ---------------------------------------------------------------- write path

  @Test
  void deterministicConversionFailureOnUpdate_quarantinesTheEvent_insteadOfWedging() {
    seedRunningSaga("saga-1");
    store.failUpdateWith = deterministicMappingFailure();
    store.failUpdateTimes = 1; // only the forward write carries the unconvertible new state

    EventEnvelope event = correlatedEnvelope("saga-1", "e1");
    assertThatCode(() -> listener.onEvents(List.of(event)))
        .as(
            "a deterministic state-conversion failure re-fails identically on every retry, so"
                + " propagating it out of onEvents wedges the whole subscription forever")
        .doesNotThrowAnyException();

    assertThat(deadLetters.all()).hasSize(1);
    assertThat(deadLetters.all().get(0).errorType())
        .isEqualTo(SagaStateSerializationException.class.getName());
    assertThat(
            backing
                .load(SagaId.of("saga-1"), SagaType.fromClass(FakeState.class), FakeState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.FAULTED);
  }

  @Test
  void deterministicConversionFailureOnCreate_quarantinesTheStartEvent() {
    store.failCreateWith = deterministicMappingFailure();

    EventEnvelope event = startEnvelope("1");
    assertThatCode(() -> listener.onEvents(List.of(event))).doesNotThrowAnyException();

    assertThat(deadLetters.all()).hasSize(1);
    assertThat(deadLetters.all().get(0).sagaId()).isEqualTo(SagaId.of("saga-1"));
  }

  // ---------------------------------------------------------------- read path

  @Test
  void deterministicConversionFailureOnLoad_quarantinesTheEvent_insteadOfWedging() {
    seedRunningSaga("saga-2");
    store.failLoadWith = deterministicMappingFailure();

    EventEnvelope event = correlatedEnvelope("saga-2", "e1");
    assertThatCode(() -> listener.onEvents(List.of(event)))
        .as(
            "doLoad has the identical laundering and the identical wedge — an undeserializable"
                + " stored state must not stop every saga on the subscription")
        .doesNotThrowAnyException();

    assertThat(deadLetters.all()).hasSize(1);
    assertThat(deadLetters.all().get(0).errorType())
        .isEqualTo(SagaStateSerializationException.class.getName());
  }

  // ---------------------------------------------------------------- the hard constraint

  @Test
  void transientKeyStoreOutageOnUpdate_stillPropagates_soTheBatchRetries() {
    seedRunningSaga("saga-3");
    store.failUpdateWith = transientKeyStoreOutage();

    EventEnvelope event = correlatedEnvelope("saga-3", "e1");
    assertThatThrownBy(() -> listener.onEvents(List.of(event)))
        .as(
            "a CryptoOperationException raised by the ENGINE is transient — the crypto backends"
                + " deliberately produce it so replay blocks and retries."
                + " Quarantining here would dead-letter events during a routine KMS outage.")
        .isInstanceOf(SagaStateSerializationException.class);

    assertThat(deadLetters.all()).isEmpty();
    assertThat(
            backing
                .load(SagaId.of("saga-3"), SagaType.fromClass(FakeState.class), FakeState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void transientKeyStoreOutageOnLoad_stillPropagates_soTheBatchRetries() {
    seedRunningSaga("saga-4");
    store.failLoadWith = transientKeyStoreOutage();

    EventEnvelope event = correlatedEnvelope("saga-4", "e1");
    assertThatThrownBy(() -> listener.onEvents(List.of(event)))
        .isInstanceOf(SagaStateSerializationException.class);
    assertThat(deadLetters.all()).isEmpty();
  }

  @Test
  void engineCryptoFailureWrappedByJackson_stillPropagates() {
    // The realistic shape: Jackson wraps a property writer's exception into a JsonMappingException
    // (an IOException), so the engine's CryptoOperationException arrives UNDERNEATH an IOException.
    // A classifier that decided on the outermost frame would bin every key-store outage as poison.
    seedRunningSaga("saga-5");
    store.failUpdateWith =
        new IOException(
            "Java object serialization failure", new CryptoOperationException("KMS unreachable"));

    EventEnvelope event = correlatedEnvelope("saga-5", "e1");
    assertThatThrownBy(() -> listener.onEvents(List.of(event)))
        .isInstanceOf(SagaStateSerializationException.class);
    assertThat(deadLetters.all()).isEmpty();
  }

  @Test
  void jdbcFailureInsideTheMapper_stillPropagates() {
    // PostgresCryptoEngine runs its own query inside the encrypting writer, so a pool/JDBC failure
    // surfaces as a SQLException on this path. Checked before the IOException rule because JDBC
    // drivers routinely wrap a socket IOException in a SQLException.
    seedRunningSaga("saga-6");
    store.failUpdateWith = new SQLException("connection pool exhausted");

    EventEnvelope event = correlatedEnvelope("saga-6", "e1");
    assertThatThrownBy(() -> listener.onEvents(List.of(event)))
        .isInstanceOf(SagaStateSerializationException.class);
    assertThat(deadLetters.all()).isEmpty();
  }

  // ------------------------------------------------- the finding's own primary scenario

  @Test
  void nullSubjectIdRefusal_isDeterministic_andQuarantines() {
    // The finding's named scenario: CryptoShreddingModule refuses to fall back to plaintext because
    // an @Encrypted component's subjectId is still null — the natural shape of a partially
    // populated saga state mid-flow. It is raised BY THE MODULE, so it is a CryptoMappingException
    // and provably deterministic, unlike the bare CryptoOperationException an engine raises.
    seedRunningSaga("saga-7");
    store.failUpdateWith =
        new IOException(
            "Java object serialization failure",
            new CryptoMappingException(
                "Cannot encrypt field 'email' on OrderSagaState: subjectId field 'customerId' is"
                    + " null"));
    store.failUpdateTimes = 1;

    EventEnvelope event = correlatedEnvelope("saga-7", "e1");
    assertThatCode(() -> listener.onEvents(List.of(event))).doesNotThrowAnyException();

    assertThat(deadLetters.all()).hasSize(1);
    assertThat(
            backing
                .load(SagaId.of("saga-7"), SagaType.fromClass(FakeState.class), FakeState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.FAULTED);
  }

  @Test
  void cryptoShreddedSubject_isDeterministic_andQuarantines() {
    // The engine refuses to re-encrypt for an erased subject and will keep refusing until an
    // operator calls reinstate() — the same "permanent, no retry can ever succeed" verdict
    // VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE already reaches for commands.
    seedRunningSaga("saga-8");
    store.failUpdateWith =
        new IOException(
            "Java object serialization failure",
            new SubjectForgottenException(
                "Subject was crypto-shredded (GDPR erasure) and cannot be re-encrypted: <redacted>"));

    EventEnvelope event = correlatedEnvelope("saga-8", "e1");
    assertThatCode(() -> listener.onEvents(List.of(event))).doesNotThrowAnyException();
    assertThat(deadLetters.all()).hasSize(1);
  }

  // ------------------------------------------------- the poison handler must not re-wedge

  @Test
  void unconvertibleStateOnTheForwardWrite_quarantines_andTheStatelessFaultWriteStillFaults() {
    // The realistic end-to-end shape of the finding: the state that failed to serialize is the same
    // state the earlier fault write wrote back, so it failed identically and the row was left
    // RUNNING. The fault write is markFaulted, which carries NO state: the
    // row IS faulted, with pre_fault_status recording what it replaced, and the quarantine entry is
    // durable either way — nothing is rethrown.
    seedRunningSaga("saga-9");
    store.failUpdateWith =
        new CryptoMappingException(
            "Cannot encrypt field 'email' on OrderSagaState: subjectId field 'customerId' is null");

    EventEnvelope event = correlatedEnvelope("saga-9", "e1");
    assertThatCode(() -> listener.onEvents(List.of(event))).doesNotThrowAnyException();

    assertThat(deadLetters.all()).hasSize(1);
    var row =
        backing
            .load(SagaId.of("saga-9"), SagaType.fromClass(FakeState.class), FakeState.class)
            .orElseThrow();
    assertThat(row.status())
        .as("markFaulted writes no state, so the unconvertible state cannot block the fault")
        .isEqualTo(SagaStatus.FAULTED);
    assertThat(row.preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.deadLetterPending()).isTrue();
  }

  @Test
  void batchContinuesPastAConversionPoison() {
    seedRunningSaga("saga-a");
    seedRunningSaga("saga-b");
    store.failUpdateWith = deterministicMappingFailure();

    EventEnvelope first = correlatedEnvelope("saga-a", "e1");
    EventEnvelope second = correlatedEnvelope("saga-b", "e2");
    assertThatCode(() -> listener.onEvents(List.of(first, second))).doesNotThrowAnyException();

    assertThat(deadLetters.all())
        .as("no head-of-line blocking: both events are recorded, the batch completes")
        .hasSize(2);
  }
}
