package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.SnapshotPolicy;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.runtime.gdpr.ForgetResult;
import org.streamrune.runtime.gdpr.ForgetSubjectService;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * A command in flight across a forget must not store a snapshot of the state it loaded before the
 * forget. The command loads the customer while the key exists, so its state holds the name in a
 * plain field; the forget deletes the key and purges the snapshots; then the command appends and
 * crosses the snapshot threshold. The snapshot it would write carries the name in the clear, after
 * the forget reported a complete erasure. The decider blocks between the load and the append, which
 * makes the interleaving deterministic.
 */
@Testcontainers
class PostgresSnapshotPurgeRaceTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  static PGSimpleDataSource dataSource;
  static SimpleEventTypeRegistry registry;

  InMemoryCryptoEngine cryptoEngine;
  PostgresEventStore store;
  VirtualThreadCommandBus bus;
  ForgetSubjectService forgetService;
  final GatedDecider decider = new GatedDecider();

  sealed interface CustomerCommand extends Command {
    String customerId();
  }

  record Register(String customerId, String name) implements CustomerCommand {}

  record Touch(String customerId) implements CustomerCommand {}

  sealed interface CustomerEvent extends DomainEvent {}

  record Registered(String customerId, @Encrypted(subjectId = "customerId") String name)
      implements CustomerEvent {}

  record Touched(String customerId) implements CustomerEvent {}

  /** The state copies the decrypted name into a plain field, as many real states do. */
  record CustomerState(String customerId, String name, int touches) implements AggregateState {}

  /** Blocks {@code decide} for a {@link Touch} while the gate is armed. */
  static final class GatedDecider
      implements Decider<CustomerCommand, CustomerState, CustomerEvent> {
    final AtomicBoolean armed = new AtomicBoolean();
    volatile CountDownLatch loaded = new CountDownLatch(1);
    volatile CountDownLatch release = new CountDownLatch(1);

    @Override
    public CustomerState initialState() {
      return new CustomerState(null, null, 0);
    }

    @Override
    public List<CustomerEvent> decide(CustomerCommand command, CustomerState state) {
      return switch (command) {
        case Register r -> List.of(new Registered(r.customerId(), r.name()));
        case Touch t -> {
          if (armed.getAndSet(false)) {
            loaded.countDown();
            await(release);
          }
          yield List.of(new Touched(t.customerId()));
        }
      };
    }

    @Override
    public CustomerState evolve(CustomerState state, CustomerEvent event) {
      return switch (event) {
        case Registered r -> new CustomerState(r.customerId(), r.name(), 0);
        case Touched t -> new CustomerState(state.customerId(), state.name(), state.touches() + 1);
      };
    }

    private static void await(CountDownLatch latch) {
      try {
        if (!latch.await(30, TimeUnit.SECONDS)) {
          throw new IllegalStateException("gate not released");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
  }

  static final org.streamrune.core.types.AggregateType CUSTOMER =
      org.streamrune.core.types.AggregateType.of("customer");

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    registry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("Registered", Registered.class)
            .registerEvent("Touched", Touched.class)
            .registerState("CustomerState", CustomerState.class)
            .build();
    new PostgresEventStoreFactory(dataSource, registry)
        .cryptoEngine(new InMemoryCryptoEngine())
        .create();
  }

  @BeforeEach
  void setUp() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM snapshot_store");
      stmt.execute("DELETE FROM event_stream");
    }
    cryptoEngine = new InMemoryCryptoEngine();
    store = (PostgresEventStore) storeWith(cryptoEngine);
    bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .snapshotPolicy(SnapshotPolicy.everyNEvents(2))
            .register(CUSTOMER, CustomerCommand.class, c -> AggregateId.of(c.customerId()), decider)
            .build();
    forgetService =
        ForgetSubjectService.builder()
            .cryptoEngine(cryptoEngine)
            .purgers(
                List.of(
                    new PostgresSnapshotStorePurger(
                        dataSource, subject -> List.of(streamOf(subject.value())))))
            .build();
  }

  private static EventStore storeWith(InMemoryCryptoEngine engine) {
    return PostgresEventStore.builder()
        .dataSource(dataSource)
        .typeRegistry(registry)
        .cryptoEngine(engine)
        .build();
  }

  private static StreamId streamOf(String customerId) {
    return StreamId.of(CUSTOMER, new AggregateId(customerId));
  }

  /** The stored state payloads of the stream: a purged stream has none. */
  private static List<String> storedStatePayloads(StreamId stream) throws Exception {
    var payloads = new java.util.ArrayList<String>();
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT state_payload::text FROM snapshot_store"
                    + " WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, stream.aggregateType().value());
      ps.setString(2, stream.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          String payload = rs.getString(1);
          if (payload != null) {
            payloads.add(payload);
          }
        }
      }
    }
    return payloads;
  }

  /**
   * Runs a {@link Touch} that loads the customer, then forgets the customer while the command is
   * held between its load and its append, then lets the command finish.
   */
  private ForgetResult forgetWhileTouchInFlight(String customerId) throws Exception {
    decider.loaded = new CountDownLatch(1);
    decider.release = new CountDownLatch(1);
    decider.armed.set(true);
    CompletableFuture<?> inFlight =
        CompletableFuture.runAsync(() -> bus.execute(new Touch(customerId)));
    assertThat(decider.loaded.await(30, TimeUnit.SECONDS)).isTrue();
    ForgetResult result = forgetService.forget(SubjectId.of(customerId), UserId.of("dpo"));
    decider.release.countDown();
    inFlight.get(30, TimeUnit.SECONDS);
    return result;
  }

  @Test
  void aCommandInFlightAcrossTheForgetStoresNoSnapshotOfThePlaintextState() throws Exception {
    bus.execute(new Register("c-alice", "Alice Example"));

    ForgetResult result = forgetWhileTouchInFlight("c-alice");

    assertThat(result.fullyErased()).isTrue();
    assertThat(storedStatePayloads(streamOf("c-alice")))
        .as("no snapshot may hold the forgotten customer's name after the forget returned")
        .noneMatch(payload -> payload.contains("Alice Example"));
  }

  @Test
  void aSnapshotStoredBeforeTheForgetIsGoneAfterIt() throws Exception {
    bus.execute(new Register("c-bob", "Bob Example"));
    bus.execute(new Touch("c-bob"));
    assertThat(storedStatePayloads(streamOf("c-bob"))).anyMatch(p -> p.contains("Bob Example"));

    forgetService.forget(SubjectId.of("c-bob"), null);

    assertThat(storedStatePayloads(streamOf("c-bob"))).isEmpty();
  }

  @Test
  void theFirstCommandLoadedAfterTheForgetSnapshotsTheRedactedState() throws Exception {
    bus.execute(new Register("c-carol", "Carol Example"));
    forgetWhileTouchInFlight("c-carol");

    bus.execute(new Touch("c-carol"));

    List<String> payloads = storedStatePayloads(streamOf("c-carol"));
    assertThat(payloads).as("the stream is snapshotted again after the forget").hasSize(1);
    assertThat(payloads.getFirst()).doesNotContain("Carol Example").contains("[REDACTED]");
  }
}
