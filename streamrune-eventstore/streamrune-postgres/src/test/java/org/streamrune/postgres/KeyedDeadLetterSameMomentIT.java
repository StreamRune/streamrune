package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.runtime.CommandDeadLetteredException;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.FaultSchedule;
import org.streamrune.test.ForwardingDeadLetterQueue;
import org.streamrune.test.ForwardingEventStore;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Two replicas dead-letter the same keyed command at the same moment, against the shipped {@code
 * dead_letter_queue} table: both look the key up and find nothing before either inserts, one insert
 * stores the row and the other stores nothing, and both callers are told the stored entry's id.
 */
@Testcontainers
class KeyedDeadLetterSameMomentIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("dlq_same_moment");

  private static final AggregateType COUNTER = AggregateType.of("counter");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    org.flywaydb.core.Flyway.configure()
        .dataSource(dataSource)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
  }

  @BeforeEach
  void clean() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM dead_letter_queue");
    }
  }

  @Test
  void twoReplicasDeadLetteringOneKeyedCommandAtOnce_storeOneRow_andBothCallersNameIt()
      throws Exception {
    for (int round = 0; round < 5; round++) {
      var key = IdempotencyKey.of("client-op-" + round);
      var bothLookedUp = new CyclicBarrier(2);
      var lookedUp = ThreadLocal.withInitial(() -> false);
      DeadLetterQueue queue =
          new ForwardingDeadLetterQueue(new PostgresDeadLetterQueue(dataSource)) {
            @Override
            public List<DeadLetterEntry> findByIdempotencyKey(IdempotencyKey k) {
              List<DeadLetterEntry> found = super.findByIdempotencyKey(k);
              if (!lookedUp.get()) {
                lookedUp.set(true);
                try {
                  bothLookedUp.await(10, TimeUnit.SECONDS);
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              }
              return found;
            }
          };
      var replicaA = replica(queue);
      var replicaB = replica(queue);
      String id = "c-" + round;

      CommandDeadLetteredException first;
      CommandDeadLetteredException second;
      try (var threads = Executors.newVirtualThreadPerTaskExecutor()) {
        var a = threads.submit(() -> deadLettered(replicaA, id, key));
        var b = threads.submit(() -> deadLettered(replicaB, id, key));
        first = a.get(30, TimeUnit.SECONDS);
        second = b.get(30, TimeUnit.SECONDS);
      }

      var stored = new PostgresDeadLetterQueue(dataSource).findByIdempotencyKey(key);
      assertThat(stored).singleElement();
      assertThat(first.commandId()).isEqualTo(stored.getFirst().commandId());
      assertThat(second.commandId()).isEqualTo(stored.getFirst().commandId());
    }
  }

  private static CommandDeadLetteredException deadLettered(
      VirtualThreadCommandBus bus, String id, IdempotencyKey key) {
    Throwable thrown = catchThrowable(() -> bus.execute(new Adjust(id, 1), key));
    assertThat(thrown).isInstanceOf(CommandDeadLetteredException.class);
    return (CommandDeadLetteredException) thrown;
  }

  /** One replica: its own store, which cannot append, and inbox; the shared dead letter queue. */
  private static VirtualThreadCommandBus replica(DeadLetterQueue queue) {
    var inbox = new InMemoryCommandInbox();
    var outage =
        FaultSchedule.none()
            .throwing(() -> new EventStoreException("connection reset by peer"))
            .failAlways("appendWithKey")
            .failAlways("append");
    return VirtualThreadCommandBus.builder()
        .eventStore(
            new ForwardingEventStore(new InMemoryEventStore().withCommandInbox(inbox), outage))
        .commandInbox(inbox)
        .deadLetterQueue(queue)
        .objectMapper(new ObjectMapper())
        .register(COUNTER, Adjust.class, c -> AggregateId.of(c.id()), new CounterDecider())
        .build();
  }

  record Adjust(String id, int delta) implements Command {}

  record Adjusted(String id, int delta) implements DomainEvent {}

  record CounterState(int value) implements AggregateState {}

  static final class CounterDecider implements Decider<Adjust, CounterState, Adjusted> {
    @Override
    public CounterState initialState() {
      return new CounterState(0);
    }

    @Override
    public List<Adjusted> decide(Adjust command, CounterState state) {
      return List.of(new Adjusted(command.id(), command.delta()));
    }

    @Override
    public CounterState evolve(CounterState state, Adjusted event) {
      return new CounterState(state.value() + event.delta());
    }
  }
}
