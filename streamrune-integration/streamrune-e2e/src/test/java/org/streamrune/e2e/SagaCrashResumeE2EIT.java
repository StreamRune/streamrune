package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.postgres.PostgresCommandInbox;
import org.streamrune.postgres.PostgresEventStore;
import org.streamrune.postgres.PostgresOffsetStore;
import org.streamrune.postgres.PostgresSagaStore;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemorySagaDeadLetterStore;

/**
 * The first end-to-end saga integration test on <em>real</em> infrastructure — a real {@link
 * PollingEventSubscription} feeding a {@link SagaRunner} backed by a real {@link
 * PostgresSagaStore}, a {@link VirtualThreadCommandBus} whose keyed dispatch goes through a real
 * {@link PostgresCommandInbox} + {@link PostgresEventStore}, and a real {@link
 * PostgresOffsetStore}.
 *
 * <p>Every prior saga race/crash proof runs on in-memory doubles; this composes the whole loop on
 * PostgreSQL so a composition defect (exception mapping around the claim, timestamptz rounding,
 * transaction semantics of {@code appendWithKey} during a saga batch) would surface. It also guards
 * the claim-first fix end-to-end: a start event whose forward command fails must reach a terminal
 * {@code COMPENSATED} with its compensation recorded in the command inbox <em>exactly once</em> and
 * its compensating event appended exactly once — never an orphaned compensation.
 *
 * <p>Postgres-only — runs in the default {@code test} task.
 */
@Timeout(120)
class SagaCrashResumeE2EIT extends E2ETestBase {

  // ---- domain events ----
  record OrderPlaced(String orderId) implements DomainEvent {}

  record OrderCancelled(String orderId) implements DomainEvent {}

  // ---- commands ----
  /** Forward command that always fails, so the start-event path must compensate. */
  record ReserveStock(String orderId) implements Command {}

  /** Compensation command that succeeds, appending {@link OrderCancelled}. */
  record CancelOrder(String orderId) implements Command {}

  // ---- ReserveStock aggregate: its decider always throws (transient infra failure) ----
  record StockState() implements AggregateState {}

  static final class ReserveDecider implements Decider<ReserveStock, StockState, DomainEvent> {
    @Override
    public StockState initialState() {
      return new StockState();
    }

    @Override
    public List<DomainEvent> decide(ReserveStock command, StockState state) {
      throw new RuntimeException("stock reservation temporarily unavailable");
    }

    @Override
    public StockState evolve(StockState state, DomainEvent event) {
      return state;
    }
  }

  // ---- CancelOrder aggregate: appends OrderCancelled (compensation) ----
  record CancelState() implements AggregateState {}

  static final class CancelDecider implements Decider<CancelOrder, CancelState, OrderCancelled> {
    @Override
    public CancelState initialState() {
      return new CancelState();
    }

    @Override
    public List<OrderCancelled> decide(CancelOrder command, CancelState state) {
      return List.of(new OrderCancelled(command.orderId()));
    }

    @Override
    public CancelState evolve(CancelState state, OrderCancelled event) {
      return state;
    }
  }

  // ---- saga ----
  record OrderSagaState(SagaStatus status, String orderId) implements SagaState {
    @JsonCreator
    OrderSagaState(
        @JsonProperty("status") SagaStatus status, @JsonProperty("orderId") String orderId) {
      this.status = status;
      this.orderId = orderId;
    }

    @Override
    public SagaStatus status() {
      return status;
    }
  }

  static final class OrderSaga implements SagaOrchestrator<OrderSagaState> {
    @Override
    public Class<OrderSagaState> stateType() {
      return OrderSagaState.class;
    }

    @Override
    public OrderSagaState initialState(SagaId sagaId) {
      return new OrderSagaState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof OrderPlaced;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("saga-" + ((OrderPlaced) event.event()).orderId());
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      String corr = event.metadata().correlationId().value();
      return corr.startsWith("saga-") ? Optional.of(SagaId.of(corr)) : Optional.empty();
    }

    @Override
    public OrderSagaState evolve(OrderSagaState state, EventEnvelope event) {
      if (event.event() instanceof OrderPlaced p) {
        return new OrderSagaState(SagaStatus.RUNNING, p.orderId());
      }
      return state;
    }

    @Override
    public List<SagaCommand> handle(OrderSagaState state, EventEnvelope event) {
      if (state.status() == SagaStatus.RUNNING && event.event() instanceof OrderPlaced p) {
        return List.of(
            SagaCommand.of(new ReserveStock(p.orderId()), AggregateId.of("stock-" + p.orderId())));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        OrderSagaState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(
          SagaCommand.of(
              new CancelOrder(state.orderId()), AggregateId.of("cancel-" + state.orderId())));
    }
  }

  /** Registry resolving the saga's event types for the event store's read path. */
  private static final EventTypeRegistry SAGA_REGISTRY =
      new EventTypeRegistry() {
        @Override
        public Class<?> resolveEventType(EventType eventType) {
          if (EventType.fromClass(OrderPlaced.class).equals(eventType)) return OrderPlaced.class;
          if (EventType.fromClass(OrderCancelled.class).equals(eventType))
            return OrderCancelled.class;
          throw new IllegalArgumentException("Unknown event type: " + eventType);
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          throw new IllegalArgumentException("No aggregate snapshots in this test: " + stateType);
        }
      };

  private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

  private PostgresCommandInbox commandInbox;
  private PostgresEventStore store;
  private PostgresSagaStore sagaStore;
  private PostgresOffsetStore offsetStore;

  @BeforeEach
  void cleanSagaTables() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM saga_state");
      stmt.execute("DELETE FROM command_inbox");
    }
    commandInbox = new PostgresCommandInbox(dataSource);
    store =
        new PostgresEventStore(
            dataSource, mapper, SAGA_REGISTRY, null, null, null, null, commandInbox);
    sagaStore = new PostgresSagaStore(dataSource, mapper);
    offsetStore = new PostgresOffsetStore(dataSource);
  }

  private VirtualThreadCommandBus commandBus() {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .locker(new LocalStripedLocker(16))
        // The forward command's failure is permanent for one execution — fail fast to compensation.
        .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
        .objectMapper(mapper)
        .commandInbox(commandInbox)
        .register(
            AggregateType.of("stock"),
            ReserveStock.class,
            cmd -> AggregateId.of("stock-" + cmd.orderId()),
            new ReserveDecider())
        .register(
            AggregateType.of("cancel"),
            CancelOrder.class,
            cmd -> AggregateId.of("cancel-" + cmd.orderId()),
            new CancelDecider())
        .build();
  }

  private void appendOrderPlaced(String orderId) {
    StreamId streamId = StreamId.of(AggregateType.of("order"), AggregateId.of(orderId));
    EventEnvelope envelope =
        new EventEnvelope(
            GlobalOffset.initial(),
            streamId,
            new Version(1),
            EventType.fromClass(OrderPlaced.class),
            new OrderPlaced(orderId),
            new EventMetadata(
                EventId.of("evt-" + UUID.randomUUID()),
                CommandId.of("cmd-" + UUID.randomUUID()),
                null,
                null,
                CorrelationId.of("order-place-" + orderId),
                null,
                null,
                Instant.now()));
    store.append(streamId, List.of(envelope), Version.initial());
  }

  private long count(String sql, String param) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(sql)) {
      ps.setString(1, param);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  private Optional<String> sagaStatus(String sagaId) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT status FROM saga_state WHERE saga_id = ?")) {
      ps.setString(1, sagaId);
      try (var rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rs.getString(1)) : Optional.empty();
      }
    }
  }

  @Test
  void startEventForwardFailure_compensatesToTerminal_onRealInfra_exactlyOnce() throws Exception {
    String orderId = "o-" + UUID.randomUUID();
    String sagaId = "saga-" + orderId;
    var bus = commandBus();
    var runner =
        SagaRunner.<OrderSagaState>builder()
            .orchestrator(new OrderSaga())
            .sagaStore(sagaStore)
            .commandBus(bus)
            // The dead-letter store must READ AND WRITE the saga rows it shields
            // (SagaDeadLetterStore's class javadoc). Wiring it to THIS Postgres saga store is what
            // makes a hold's shield, a drain's conditional clear and retention's owner guard answer
            // about the rows under test.
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();

    // A real subscription drives the whole loop off the global event stream.
    var subscription =
        new PollingEventSubscription(
            SubscriptionName.of("saga-e2e"),
            store,
            offsetStore,
            runner.asEventListener(),
            fastConfig(),
            50);

    // Publish the start event. The subscription picks it up → the saga starts → ReserveStock fails
    // → claim-first COMPENSATING → CancelOrder succeeds → COMPENSATED.
    appendOrderPlaced(orderId);
    subscription.start();
    try {
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(sagaStatus(sagaId))
                      .as("the saga reaches a terminal COMPENSATED on the real store")
                      .contains(SagaStatus.COMPENSATED.name()));
    } finally {
      subscription.close();
    }

    // The compensation command was recorded in the real command inbox under its episode-scoped key
    // EXACTLY ONCE — the claim-first fix guarantees it is never orphaned or double-executed.
    //
    // Episode version 2, not 1, now that the start path is create-FIRST. The row
    // is written genesis-pending at version 1 BEFORE the forward step runs, so the COMPENSATING
    // claim that follows ReserveStock's failure CASes that row to version 2 and stamps the episode
    // there. Previously the row was created only after the dispatch loop, so the claim's own create
    // produced version 1 — the number this assertion was written against. The saga still reaches
    // COMPENSATED with exactly one compensation command; only the episode's version moved, and the
    // count assertion below (one key in total for this saga) is what pins "exactly once".
    String episodeKey = "saga:" + sagaId + ":episode:2:comp:0";
    assertThat(count("SELECT COUNT(*) FROM command_inbox WHERE idempotency_key = ?", episodeKey))
        .as("the compensation episode key is recorded exactly once")
        .isEqualTo(1L);

    // The failed forward command left NO inbox row (a failed keyed append records nothing), so the
    // ONLY key recorded for this saga is the compensation episode key — a committed forward step
    // would make this count 2.
    assertThat(
            count(
                "SELECT COUNT(*) FROM command_inbox WHERE idempotency_key LIKE ?",
                "saga:" + sagaId + ":%"))
        .as("only the compensation key is present — the failed ReserveStock recorded nothing")
        .isEqualTo(1L);

    // The compensating event was appended exactly once (no double compensation).
    assertThat(
            count(
                "SELECT COUNT(*) FROM event_stream WHERE event_type = ?",
                EventType.fromClass(OrderCancelled.class).name()))
        .as("compensation executed exactly once — OrderCancelled appended once")
        .isEqualTo(1L);
  }
}
