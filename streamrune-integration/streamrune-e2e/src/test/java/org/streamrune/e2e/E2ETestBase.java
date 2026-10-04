package org.streamrune.e2e;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.postgres.PostgresEventStore;
import org.streamrune.postgres.PostgresEventStoreFactory;
import org.streamrune.postgres.PostgresOutboxStore;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Shared base for the end-to-end crash-recovery / HA tests.
 *
 * <p>Starts ONE {@link PostgresTestImage#NAME PostgreSQL} container for the whole JVM (a static
 * singleton started once and intentionally never stopped — Testcontainers' Ryuk reaper removes it
 * when the JVM exits, matching the project's existing integration tests) and initializes the
 * framework schema the same way the streamrune-postgres tests do: {@link
 * PostgresEventStoreFactory#create()} runs the bundled Flyway migrations from {@code
 * classpath:db/streamrune-migration} (the postgres module ships them) and returns a HikariCP-pooled
 * {@link DataSource}. No schema is hand-written here.
 *
 * <p>Subclasses get a real pooled {@link #dataSource}, factories for a {@link PostgresEventStore}
 * (with an outbox store wired so the transactional-outbox append path works) and a {@link
 * PostgresOutboxStore}, plus helpers to append {@link CountEvent}s. {@link #cleanDatabase()} runs
 * before each test so scenarios start from an empty store while sharing the container.
 */
abstract class E2ETestBase {

  /**
   * One container for all e2e tests. {@code @Container} is intentionally NOT used: a JUnit-managed
   * static container is per-class, so each IT would spin up its own Postgres. A plain static
   * singleton started in {@link #startContainer()} is shared across every IT in the module.
   */
  static final PostgreSQLContainer<?> POSTGRES =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_e2e");

  /**
   * Source pointed straight at the container. A {@link PGSimpleDataSource} opens a fresh physical
   * connection per {@code getConnection()} — no pool to exhaust under the concurrent HA scenario,
   * and it is exactly what the existing streamrune-postgres integration tests use.
   */
  protected static DataSource dataSource;

  /** Registry that resolves {@link CountEvent} for the event store's payload (de)serialization. */
  protected static final EventTypeRegistry TYPE_REGISTRY =
      new EventTypeRegistry() {
        @Override
        public Class<?> resolveEventType(EventType eventType) {
          if (CountEvent.TYPE.equals(eventType.name())) {
            return CountEvent.class;
          }
          throw new IllegalArgumentException("Unknown event type: " + eventType);
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          throw new IllegalArgumentException("No state types registered: " + stateType);
        }

        @Override
        public java.util.Collection<Class<?>> registeredTypes() {
          return java.util.List.of(CountEvent.class);
        }
      };

  private static final ObjectMapper MAPPER =
      new ObjectMapper().registerModule(new JavaTimeModule());

  @BeforeAll
  static void startContainer() {
    if (!POSTGRES.isRunning()) {
      POSTGRES.start();
    }
    var pgSource = new PGSimpleDataSource();
    pgSource.setUrl(POSTGRES.getJdbcUrl());
    pgSource.setUser(POSTGRES.getUsername());
    pgSource.setPassword(POSTGRES.getPassword());
    dataSource = pgSource;

    // Initialize the framework schema exactly like the streamrune-postgres tests: the factory runs
    // the bundled Flyway migrations (classpath:db/streamrune-migration) and validates the result.
    // We discard
    // the returned EventStore — the tests build their own stores against `dataSource` so they can
    // wrap it (fault injection) and run multiple independent instances against one database.
    new PostgresEventStoreFactory(dataSource, TYPE_REGISTRY).create();
  }

  @BeforeEach
  void cleanDatabase() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM outbox_events");
      stmt.execute("DELETE FROM event_stream");
      // Reset the gapless global-offset counter so each test's offsets restart at 1, the same
      // dense-from-1 state a fresh production database has. A plain DELETE leaves the counter
      // advanced from a previous test method, which would put the stream's first offset well above
      // a fresh projection's checkpoint 0 — a state production never reaches (event_stream is
      // never deleted) and which the reader's contiguity guard correctly treats as a hole.
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
      stmt.execute("DELETE FROM projection_offset");
      // Clear leases so no test inherits a live lease (and its epoch numbering) from a previous
      // method — a stale live lease would make a later lease-based IT's acquire silently fail.
      stmt.execute("DELETE FROM subscription_leases");
      // Projection read-model tables are created lazily per projection name (e.g.
      // <name>_view); drop any left over from a previous test so row-count assertions are exact.
      try (var rs =
          stmt.executeQuery(
              "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
                  + "AND tablename LIKE '%\\_view'")) {
        var views = new ArrayList<String>();
        while (rs.next()) {
          views.add(rs.getString(1));
        }
        for (String view : views) {
          stmt.execute("DROP TABLE IF EXISTS " + view);
        }
      }
    }
  }

  // ---- Event store / outbox factories ------------------------------------------------------

  /**
   * A {@link PostgresEventStore} bound to {@code source}, with an outbox store wired so the
   * transactional-outbox append path is available. Gap-free global-offset ordering for
   * offset-checkpointing readers is guaranteed by the read-side transaction-id watermark on {@code
   * readGlobalStream}, not by an append lock.
   */
  protected static PostgresEventStore eventStore(
      DataSource source, PostgresOutboxStore outboxStore) {
    return new PostgresEventStore(
        source,
        MAPPER,
        TYPE_REGISTRY,
        /* upcasterChain= */ null,
        /* auditStore= */ null,
        outboxStore,
        /* outboxEventMapper= */ null);
  }

  /** A {@link PostgresEventStore} with no outbox store (sufficient for projection scenarios). */
  protected static PostgresEventStore eventStore(DataSource source) {
    return PostgresEventStore.builder().dataSource(source).typeRegistry(TYPE_REGISTRY).build();
  }

  // ---- Event helpers -------------------------------------------------------------------------

  /**
   * Minimal domain event carrying a sequence number, so projections can detect loss/duplication.
   */
  record CountEvent(int n) implements DomainEvent {
    static final String TYPE = "CountEvent";
  }

  /**
   * Polling-only subscription config with a short interval, so catch-up and live polling cycle
   * quickly under test without relying on LISTEN/NOTIFY timing.
   */
  protected static org.streamrune.core.subscription.SubscriptionConfig fastConfig() {
    return org.streamrune.core.subscription.SubscriptionConfig.pollingOnly(
        java.time.Duration.ofMillis(50));
  }

  private static EventMetadata metadata(int n) {
    return new EventMetadata(
        IdGenerator.generateEventId(),
        IdGenerator.generateCommandId(),
        null,
        null,
        CorrelationId.of("e2e-corr-" + n),
        null,
        null,
        Instant.now());
  }

  /**
   * Appends {@code count} {@link CountEvent}s (n = startN .. startN+count-1) to {@code streamId}
   * starting at {@code expectedVersion}, on the given store. Returns the assigned global offsets.
   */
  protected static List<GlobalOffset> appendCountEvents(
      PostgresEventStore store, StreamId streamId, long expectedVersion, int startN, int count) {
    var envelopes = new ArrayList<EventEnvelope>(count);
    long v = expectedVersion;
    for (int i = 0; i < count; i++) {
      v++;
      envelopes.add(
          new EventEnvelope(
              GlobalOffset.initial(),
              streamId,
              new Version(v),
              new EventType(CountEvent.TYPE),
              new CountEvent(startN + i),
              metadata(startN + i)));
    }
    return store.append(streamId, envelopes, new Version(expectedVersion)).globalOffsets();
  }

  // ---- Counting projection -----------------------------------------------------------------

  /**
   * Read-model row written one-per-event by {@link OffsetCountingProjection}, keyed by the event's
   * global offset. {@code applyCount} records how many times this exact offset was written through
   * the (transactional) repository — a value &gt; 1 means the same committed event was applied more
   * than once, which exactly-once at the runner level must prevent.
   */
  record AppliedRow(long offset, int n, int applyCount) {}

  /**
   * A projection that records, per global offset, every time it is asked to apply that event — both
   * in memory (for loss/duplication checks against the {@code process()} contract) and persisted
   * through the transaction-scoped {@link org.streamrune.core.projection.ProjectionRepository} so
   * the read model is exactly-once-effective.
   *
   * <p>Drive it through {@link org.streamrune.postgres.JdbcProjectionRepository} as the runner's
   * {@code AtomicBatchProcessor}: each batch's writes and the offset save then commit (or roll
   * back) in one transaction, so a committed offset N guarantees offsets ≤ N are applied to the
   * read model exactly once.
   */
  static final class OffsetCountingProjection implements org.streamrune.core.projection.Projection {

    private final ProjectionName projectionName;
    // offset -> number of process() invocations carrying that offset (in-memory, across retries).
    final java.util.concurrent.ConcurrentHashMap<Long, java.util.concurrent.atomic.AtomicInteger>
        inMemoryApplications = new java.util.concurrent.ConcurrentHashMap<>();
    final java.util.concurrent.atomic.AtomicInteger totalProcessCalls =
        new java.util.concurrent.atomic.AtomicInteger();

    OffsetCountingProjection(String projectionName) {
      this.projectionName = ProjectionName.of(projectionName);
    }

    OffsetCountingProjection(ProjectionName projectionName) {
      this.projectionName = projectionName;
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      throw new IllegalStateException(
          "drive through the repository overload (transactional AtomicBatchProcessor)");
    }

    @Override
    public void process(
        List<EventEnvelope> batch, org.streamrune.core.projection.ProjectionRepository repository) {
      totalProcessCalls.incrementAndGet();
      for (EventEnvelope env : batch) {
        long offset = env.globalOffset().value();
        inMemoryApplications
            .computeIfAbsent(offset, k -> new java.util.concurrent.atomic.AtomicInteger())
            .incrementAndGet();

        int n = ((CountEvent) env.event()).n();
        // Read-modify-write the per-offset row through the transaction-scoped repository so the
        // persisted applyCount survives only committed applications. Keyed by offset => idempotent.
        String id = Long.toString(offset);
        int priorApplyCount =
            repository
                .findById(projectionName, id, AppliedRow.class)
                .map(AppliedRow::applyCount)
                .orElse(0);
        repository.save(projectionName, id, new AppliedRow(offset, n, priorApplyCount + 1));
      }
    }
  }
}
