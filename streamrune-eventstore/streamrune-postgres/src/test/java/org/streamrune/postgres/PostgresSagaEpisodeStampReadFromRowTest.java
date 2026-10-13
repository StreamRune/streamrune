package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
 * start event and whose forward command is then rejected. The genesis-pending insert stamps the
 * episode at version 1 and the claim that follows lands the row at version 2 and keeps the stamp.
 * The delivery that opens the episode and a redelivery that resumes it both key the compensation
 * commands with the stamp the row carries, so the command inbox answers for the command the first
 * attempt committed and no compensation is stored twice.
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
  private static final SagaId SAGA = SagaId.of("order-saga-" + ORDER);
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

  @Test
  void theOpeningDeliveryAndARedelivery_keyTheCompensationCommandsWithTheRowsStamp() {
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
    InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    InMemoryEventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
    SagaStore sagaStore =
        new PostgresSagaStore(dataSource, new ObjectMapper().registerModule(new JavaTimeModule()));
    var reserve = new OneEventDecider<Reserve>(c -> new Reserved(c.orderId()));
    var release = new OneEventDecider<Release>(c -> new Released(c.orderId()));
    var refund = new OneEventDecider<Refund>(c -> new Refunded(c.orderId()));
    try (VirtualThreadCommandBus realBus =
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
                AggregateType.of("refund"), Refund.class, c -> AggregateId.of(c.orderId()), refund)
            .build()) {
      KeyRecordingBus bus = new KeyRecordingBus(realBus);
      SagaRunner<State> runner =
          SagaRunner.<State>builder()
              .decider(new BornCompensatingSaga())
              .startWhen(e -> e.event() instanceof Opened)
              .extractSagaId(e -> SAGA)
              .correlateBy(e -> Optional.empty())
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
              .build();
      // First attempt: the forward command is rejected, the release commits, the refund fails on
      // infrastructure, so the saga is left COMPENSATING with the release stored once.
      reserve.failure = () -> new IllegalArgumentException("out of stock");
      refund.failure = () -> new IllegalStateException("connection reset");
      EventEnvelope opened = append(eventStore, new Opened(ORDER));
      runner.asEventListener().onEvents(List.of(opened));
      refund.failure = null;

      LoadedSaga<State> row = sagaStore.load(SAGA, TYPE, State.class).orElseThrow();
      List<String> firstAttempt = bus.keys.stream().filter(k -> !k.startsWith("Reserve ")).toList();
      assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(row.version()).as("the insert, then the claim").isEqualTo(2L);
      assertThat(row.episodeVersion()).as("the insert's stamp, kept by the claim").isEqualTo(1L);
      String scope = "saga:" + SAGA.value() + ":episode:" + row.episodeVersion() + ":comp:";
      assertThat(firstAttempt)
          .as("the delivery that opens the episode keys its commands with the row's stamp")
          .containsExactly("Release " + scope + 0, "Refund " + scope + 1);
      assertThat(count(eventStore, Released.class)).isEqualTo(1);

      // Resume by the redelivery of the start event.
      bus.keys.clear();
      runner.asEventListener().onEvents(List.of(opened));

      List<String> resume = List.copyOf(bus.keys);
      LoadedSaga<State> after = sagaStore.load(SAGA, TYPE, State.class).orElseThrow();

      assertThat(after.status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(resume)
          .as("a resume keys the episode's compensation commands as the first attempt did")
          .containsExactlyElementsOf(firstAttempt);
      assertThat(count(eventStore, Released.class))
          .as("the release committed by the first attempt is not executed a second time")
          .isEqualTo(1);
      assertThat(count(eventStore, Refunded.class)).isEqualTo(1);
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
