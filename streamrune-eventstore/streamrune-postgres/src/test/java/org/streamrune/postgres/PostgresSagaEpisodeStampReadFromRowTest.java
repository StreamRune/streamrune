package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
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
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * On {@link PostgresSagaStore}: a saga whose own {@code evolve} yields {@code COMPENSATING} on its
 * start event.
 *
 * <ul>
 *   <li>Its row is inserted genesis-pending with a forward status; a retry-later failure of the
 *       forward command leaves that row, and the redelivery dispatches the command again and only
 *       then enters the episode, stamped by the event write with the start event as its cause.
 *   <li>When the forward command is rejected, the claim is the write that stamps the episode, and
 *       the delivery that opened it and a redelivery that resumes it key the compensation commands
 *       with that stamp.
 *   <li>A claim over a row that already carries a stamp keeps it, and the commands are keyed with
 *       the stamp the row carries, not with the version the claim landed at: the executor reads the
 *       stamp and never computes it.
 * </ul>
 */
@Testcontainers
class PostgresSagaEpisodeStampReadFromRowTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_episode_key");

  record Opened(String orderId) implements DomainEvent {}

  record Reserve(String orderId) implements Command {}

  record Release(String orderId) implements Command {}

  record Refund(String orderId) implements Command {}

  record Reserved(String orderId) implements DomainEvent {}

  record Released(String orderId) implements DomainEvent {}

  record Refunded(String orderId) implements DomainEvent {}

  record Untouched() implements AggregateState {}

  record State(SagaStatus status, String orderId) implements SagaState {}

  private static final String ORDER = "42";
  private static final SagaType TYPE = SagaType.fromClass(State.class);

  static final class OneEventDecider<C extends Command>
      implements Decider<C, Untouched, DomainEvent> {
    private final Function<C, DomainEvent> eventOf;
    volatile Supplier<RuntimeException> failure;

    OneEventDecider(Function<C, DomainEvent> eventOf) {
      this.eventOf = eventOf;
    }

    @Override
    public Untouched initialState() {
      return new Untouched();
    }

    @Override
    public List<DomainEvent> decide(C command, Untouched state) {
      Supplier<RuntimeException> failing = failure;
      if (failing != null) {
        throw failing.get();
      }
      return List.of(eventOf.apply(command));
    }

    @Override
    public Untouched evolve(Untouched state, DomainEvent event) {
      return state;
    }
  }

  /** The start event evolves to COMPENSATING and still issues a forward command. */
  static final class BornCompensatingSaga implements SagaDecider<State> {
    @Override
    public Class<State> stateType() {
      return State.class;
    }

    @Override
    public State initialState(SagaId sagaId) {
      return new State(SagaStatus.STARTED, null);
    }

    @Override
    public State evolve(State state, EventEnvelope event) {
      return new State(SagaStatus.COMPENSATING, ORDER);
    }

    @Override
    public List<SagaCommand> handle(State state, EventEnvelope event) {
      return List.of(SagaCommand.of(new Reserve(ORDER), AggregateId.of(ORDER)));
    }

    @Override
    public List<SagaCommand> compensate(State state, Throwable failure, SagaCommand failedCommand) {
      return List.of(
          SagaCommand.of(new Release(ORDER), AggregateId.of(ORDER)),
          SagaCommand.of(new Refund(ORDER), AggregateId.of(ORDER)));
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofMinutes(5));
    }
  }

  /** Records the idempotency key of every keyed command, then runs it on the real bus. */
  static final class KeyRecordingBus implements CommandBus {
    private final CommandBus delegate;
    final List<String> keys = new ArrayList<>();

    KeyRecordingBus(CommandBus delegate) {
      this.delegate = delegate;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return delegate.execute(command);
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      keys.add(command.getClass().getSimpleName() + " " + key.value());
      return delegate.execute(command, key);
    }

    @Override
    public boolean supportsIdempotentExecution() {
      return delegate.supportsIdempotentExecution();
    }
  }

  /** One saga on the PostgreSQL saga store, an in-memory event store and the real command bus. */
  private static final class Fixture implements AutoCloseable {
    final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    final InMemoryEventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
    final SagaStore sagaStore;
    final SagaId sagaId = SagaId.of("order-saga-" + UUID.randomUUID());
    final OneEventDecider<Reserve> reserve = new OneEventDecider<>(c -> new Reserved(c.orderId()));
    final OneEventDecider<Release> release = new OneEventDecider<>(c -> new Released(c.orderId()));
    final OneEventDecider<Refund> refund = new OneEventDecider<>(c -> new Refunded(c.orderId()));
    final VirtualThreadCommandBus realBus;
    final KeyRecordingBus bus;
    final SagaRunner<State> runner;

    Fixture() {
      var dataSource = new PGSimpleDataSource();
      dataSource.setUrl(PG.getJdbcUrl());
      dataSource.setUser(PG.getUsername());
      dataSource.setPassword(PG.getPassword());
      new PostgresEventStoreFactory(
              dataSource,
              new EventTypeRegistry() {
                @Override
                public Class<?> resolveEventType(EventType eventType) {
                  return Object.class;
                }

                @Override
                public Class<?> resolveStateType(String stateType) {
                  return Object.class;
                }

                @Override
                public Collection<Class<?>> registeredTypes() {
                  return List.of();
                }
              })
          .create();
      sagaStore =
          new PostgresSagaStore(
              dataSource, new ObjectMapper().registerModule(new JavaTimeModule()));
      realBus =
          VirtualThreadCommandBus.builder()
              .eventStore(eventStore)
              .locker(new LocalStripedLocker(16))
              .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
              .objectMapper(new ObjectMapper())
              .commandInbox(inbox)
              .register(
                  AggregateType.of("reservation"),
                  Reserve.class,
                  c -> AggregateId.of(c.orderId()),
                  reserve)
              .register(
                  AggregateType.of("release"),
                  Release.class,
                  c -> AggregateId.of(c.orderId()),
                  release)
              .register(
                  AggregateType.of("refund"),
                  Refund.class,
                  c -> AggregateId.of(c.orderId()),
                  refund)
              .build();
      bus = new KeyRecordingBus(realBus);
      runner =
          SagaRunner.<State>builder()
              .decider(new BornCompensatingSaga())
              .startWhen(e -> e.event() instanceof Opened)
              .extractSagaId(e -> sagaId)
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
              // The rows live in another store than the event stream; the fence is tested in
              // SagaForwardFence*Test.
              .forwardFence(false)
              .build();
    }

    LoadedSaga<State> row() {
      return sagaStore.load(sagaId, TYPE, State.class).orElseThrow();
    }

    void deliver(EventEnvelope event) {
      runner.asEventListener().onEvents(List.of(event));
    }

    List<String> compensationKeys() {
      return bus.keys.stream().filter(k -> !k.startsWith("Reserve ")).toList();
    }

    /** The two compensation keys the row's stamp gives, in dispatch order. */
    List<String> keysOfTheRowsEpisode() {
      String scope = "saga:" + sagaId.value() + ":episode:" + row().episodeVersion() + ":comp:";
      return List.of("Release " + scope + 0, "Refund " + scope + 1);
    }

    @Override
    public void close() {
      realBus.close();
    }
  }

  @Test
  void aRetryLaterFailureOfTheForwardCommand_theRedeliveryDispatchesItAgain_thenEntersTheEpisode() {
    try (var f = new Fixture()) {
      f.reserve.failure = () -> new IllegalStateException(new SQLException("connection reset"));
      EventEnvelope opened = append(f.eventStore, new Opened(ORDER));

      assertThatThrownBy(() -> f.deliver(opened)).isInstanceOf(IllegalStateException.class);

      LoadedSaga<State> pending = f.row();
      assertThat(pending.status())
          .as("a forward status until the loop ends")
          .isEqualTo(SagaStatus.STARTED);
      assertThat(pending.state().status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(pending.version()).isEqualTo(1L);
      assertThat(pending.genesisApplied()).isFalse();
      assertThat(pending.episodeVersion()).isNull();
      assertThat(pending.episodeCauseEventId()).isNull();
      assertThat(count(f.eventStore, Reserved.class)).isZero();
      List<String> firstAttempt = List.copyOf(f.bus.keys);
      f.reserve.failure = null;
      f.bus.keys.clear();

      f.deliver(opened);

      assertThat(f.bus.keys)
          .as("the same forward key, no compensation yet")
          .isEqualTo(firstAttempt);
      assertThat(count(f.eventStore, Reserved.class)).isEqualTo(1);
      LoadedSaga<State> entered = f.row();
      assertThat(entered.status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(entered.version()).isEqualTo(2L);
      assertThat(entered.genesisApplied()).isTrue();
      assertThat(entered.episodeVersion()).as("stamped by the event write").isEqualTo(2L);
      assertThat(entered.episodeCauseEventId()).isEqualTo(opened.metadata().eventId());

      f.bus.keys.clear();
      f.deliver(opened);

      assertThat(f.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(f.compensationKeys()).isEqualTo(f.keysOfTheRowsEpisode());
      assertThat(count(f.eventStore, Released.class)).isEqualTo(1);
      assertThat(count(f.eventStore, Refunded.class)).isEqualTo(1);
    }
  }

  @Test
  void theForwardCommandIsRejected_theClaimStampsTheEpisode_andARedeliveryPresentsTheSameKeys() {
    try (var f = new Fixture()) {
      // The forward command is rejected, the release commits, the refund fails on infrastructure:
      // the saga is left COMPENSATING with the release stored once.
      f.reserve.failure = () -> new IllegalArgumentException("out of stock");
      f.refund.failure = () -> new IllegalStateException("connection reset");
      EventEnvelope opened = append(f.eventStore, new Opened(ORDER));
      f.deliver(opened);
      f.refund.failure = null;

      LoadedSaga<State> row = f.row();
      assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(row.version()).as("the insert, then the claim").isEqualTo(2L);
      assertThat(row.episodeVersion()).as("the claim is the stamping write").isEqualTo(2L);
      assertThat(row.episodeCauseEventId()).isEqualTo(opened.metadata().eventId());
      assertThat(f.compensationKeys())
          .as("the delivery that opens the episode keys its commands with the row's stamp")
          .isEqualTo(f.keysOfTheRowsEpisode());
      assertThat(count(f.eventStore, Released.class)).isEqualTo(1);

      f.bus.keys.clear();
      f.deliver(opened);

      assertThat(f.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(f.compensationKeys())
          .as("a resume keys the episode's compensation commands as the first attempt did")
          .isEqualTo(f.keysOfTheRowsEpisode());
      assertThat(count(f.eventStore, Released.class))
          .as("the release committed by the first attempt is not executed a second time")
          .isEqualTo(1);
      assertThat(count(f.eventStore, Refunded.class)).isEqualTo(1);
    }
  }

  /**
   * A genesis-pending row that carries a stamp and is in a forward status — a write the store
   * contract rules out, prepared here by hand — meets the claim of a start delivery whose forward
   * command is rejected. The claim lands the row at version 3 and keeps the stamp of version 1; the
   * opening delivery and the redelivery key with the stamp the row carries and store its cause.
   */
  @Test
  void aClaimOverARowThatAlreadyCarriesAStamp_keysWithTheRowsStamp_notWithTheClaimsVersion() {
    try (var f = new Fixture()) {
      State state = new State(SagaStatus.RUNNING, ORDER);
      f.sagaStore.createGenesisPending(
          f.sagaId, TYPE, state, SagaStatus.COMPENSATING, false, EventId.of("evt-earlier"));
      f.sagaStore.update(f.sagaId, TYPE, state, SagaStatus.RUNNING, 1L);
      f.reserve.failure = () -> new IllegalArgumentException("out of stock");
      f.refund.failure = () -> new IllegalStateException("connection reset");
      EventEnvelope opened = append(f.eventStore, new Opened(ORDER));

      f.deliver(opened);
      f.refund.failure = null;

      LoadedSaga<State> row = f.row();
      assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(row.version()).as("insert, update, claim").isEqualTo(3L);
      assertThat(row.episodeVersion()).as("the claim keeps the stamp it found").isEqualTo(1L);
      assertThat(f.compensationKeys()).isEqualTo(f.keysOfTheRowsEpisode());

      f.bus.keys.clear();
      f.deliver(opened);

      assertThat(f.row().status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(f.compensationKeys()).isEqualTo(f.keysOfTheRowsEpisode());
      assertThat(count(f.eventStore, Released.class)).isEqualTo(1);
      assertThat(count(f.eventStore, Refunded.class)).isEqualTo(1);
      assertThat(
              f.eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
                  .filter(e -> e.event() instanceof Released || e.event() instanceof Refunded)
                  .map(e -> e.metadata().causationId().value())
                  .distinct())
          .as("one causation id for the episode: the one the row records")
          .containsExactly("evt-earlier");
    }
  }

  private static long count(InMemoryEventStore eventStore, Class<? extends DomainEvent> type) {
    return eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
        .filter(e -> type.isInstance(e.event()))
        .count();
  }

  private static EventEnvelope append(InMemoryEventStore eventStore, DomainEvent event) {
    StreamId streamId =
        StreamId.of(AggregateType.of("trigger"), AggregateId.of("t-" + UUID.randomUUID()));
    var appended =
        eventStore.append(
            streamId,
            List.of(
                new EventEnvelope(
                    GlobalOffset.initial(),
                    streamId,
                    Version.initial(),
                    EventType.fromClass(event.getClass()),
                    event,
                    new EventMetadata(
                        EventId.of("evt-" + UUID.randomUUID()),
                        CommandId.of("cmd-" + UUID.randomUUID()),
                        null,
                        null,
                        CorrelationId.of("request-" + UUID.randomUUID()),
                        null,
                        null,
                        Instant.now()))),
            Version.initial());
    return eventStore
        .readGlobalStream(GlobalOffset.of(appended.globalOffsets().get(0).value() - 1), 1)
        .get(0);
  }
}
