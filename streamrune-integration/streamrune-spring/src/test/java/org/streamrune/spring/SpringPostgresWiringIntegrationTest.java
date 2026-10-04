package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.postgres.PostgresEventStoreFactory;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end verification against real PostgreSQL:
 *
 * <ul>
 *   <li>the auto-configured EventStore encrypts {@code @Encrypted} fields when a {@link
 *       CryptoEngine} bean is present (the bean used to be silently ignored — PII landed in the
 *       database as plaintext), and
 *   <li>{@link StreamRuneHealthIndicator} reports the actual head of the event stream (the old
 *       probe always reported {@code lastGlobalOffset = 0}).
 * </ul>
 */
@Testcontainers
class SpringPostgresWiringIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_spring_it");

  record CustomerRegistered(String customerId, @Encrypted(subjectId = "customerId") String email)
      implements DomainEvent {}

  static PGSimpleDataSource dataSource;
  static EventTypeRegistry typeRegistry;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    typeRegistry =
        SimpleEventTypeRegistry.builder()
            .registerEvent("CustomerRegistered", CustomerRegistered.class)
            .build();

    // Run Flyway migrations once for both tests. CustomerRegistered carries an @Encrypted field,
    // so CryptoConfigValidator (wired into PostgresEventStoreFactory#create) requires a
    // CryptoEngine here too — a throwaway one, since only schema init runs through this factory;
    // each @Test below builds its own EventStore (with its own crypto wiring) via the
    // ApplicationContextRunner.
    new PostgresEventStoreFactory(dataSource, typeRegistry)
        .cryptoEngine(new InMemoryCryptoEngine())
        .create();
  }

  @Test
  void autoConfiguredEventStoreEncryptsAnnotatedFieldsWhenCryptoEngineBeanPresent() {
    var streamId =
        StreamId.of(AggregateType.of("customer"), AggregateId.of("customer-" + UUID.randomUUID()));
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(DataSource.class, () -> dataSource)
        .withBean(EventTypeRegistry.class, () -> typeRegistry)
        .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
        .run(
            ctx -> {
              EventStore store = ctx.getBean(EventStore.class);
              var event = new CustomerRegistered("cust-1", "alice@example.com");
              store.append(streamId, List.of(envelope(streamId, event)), Version.initial());

              // Raw payload in PostgreSQL must NOT contain the plaintext email
              String rawPayload = rawPayload(streamId);
              assertThat(rawPayload).contains("cust-1");
              assertThat(rawPayload).doesNotContain("alice@example.com");

              // Round-trip through the same store decrypts transparently
              var history = store.load(streamId);
              var loaded = (CustomerRegistered) history.events().getFirst().event();
              assertThat(loaded.email()).isEqualTo("alice@example.com");
            });
  }

  @Test
  void healthIndicatorReportsActualHeadOfEventStream() throws Exception {
    // CustomerRegistered carries an @Encrypted field, so CryptoConfigValidator requires a
    // CryptoEngine here too, even though this test only exercises the health indicator.
    EventStore store =
        new PostgresEventStoreFactory(dataSource, typeRegistry)
            .cryptoEngine(new InMemoryCryptoEngine())
            .create();
    var indicator = new StreamRuneHealthIndicator(dataSource);

    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("TRUNCATE event_stream RESTART IDENTITY");
    }

    var emptyHealth = indicator.health();
    assertThat(emptyHealth.getStatus()).isEqualTo(Status.UP);
    assertThat(emptyHealth.getDetails()).containsEntry("eventStore.lastGlobalOffset", 0L);

    var streamId =
        StreamId.of(AggregateType.of("customer"), AggregateId.of("health-" + UUID.randomUUID()));
    var result =
        store.append(
            streamId,
            List.of(
                envelope(streamId, new CustomerRegistered("cust-2", "bob@example.com")),
                envelope(streamId, new CustomerRegistered("cust-2", "bob2@example.com"), 2)),
            Version.initial());
    long headOffset = result.globalOffsets().getLast().value();

    var health = indicator.health();
    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails()).containsEntry("eventStore.lastGlobalOffset", headOffset);
    assertThat(health.getDetails()).containsKey("eventStore.lastEventTimestamp");
  }

  @Test
  void autoConfiguredLeadershipBeanAcquires_secondContextIsStandby() {
    // This boots the real auto-config against the real Postgres container and proves the auto-wired
    // SubscriptionLeadership bean is a LeaseBasedLeadership that actually takes a DB-clocked lease:
    // the first context's bean acquires the lease for a name, and a SECOND context's auto-wired
    // bean
    // (a distinct instance with its own holder id) is denied the same name — i.e. standby — while
    // the first still holds a live lease, then wins it after the first resigns.
    var consumer = "spring-e2e-leader-" + UUID.randomUUID();
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(DataSource.class, () -> dataSource)
        .withBean(EventTypeRegistry.class, () -> typeRegistry)
        .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
        .run(
            ctxA -> {
              var leaderA =
                  ctxA.getBean(org.streamrune.core.subscription.SubscriptionLeadership.class);
              assertThat(leaderA).isInstanceOf(org.streamrune.postgres.LeaseBasedLeadership.class);
              assertThat(leaderA.tryAcquire(consumer)).isPresent();
              assertThat(leaderA.current(consumer)).isPresent();

              // A second independent context (own auto-wired leadership bean, own holder id) must
              // be
              // standby for the same name while A still holds a live lease.
              new ApplicationContextRunner()
                  .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
                  .withBean(DataSource.class, () -> dataSource)
                  .withBean(EventTypeRegistry.class, () -> typeRegistry)
                  .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
                  .run(
                      ctxB -> {
                        var leaderB =
                            ctxB.getBean(
                                org.streamrune.core.subscription.SubscriptionLeadership.class);
                        assertThat(leaderB.tryAcquire(consumer)).isEmpty(); // standby
                        assertThat(leaderB.current(consumer)).isEmpty();

                        // After A resigns (its lease expires), B can win the name — automatic
                        // failover, with a strictly greater fencing epoch than A held.
                        leaderA.resign(consumer);
                        assertThat(leaderB.tryAcquire(consumer)).isPresent();
                        leaderB.resign(consumer);
                      });
            });
  }

  private static String rawPayload(StreamId streamId) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT payload::text FROM event_stream"
                    + " WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, streamId.aggregateType().value());
      ps.setString(2, streamId.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        return rs.getString(1);
      }
    }
  }

  private static EventEnvelope envelope(StreamId streamId, DomainEvent event) {
    return envelope(streamId, event, 1);
  }

  private static EventEnvelope envelope(StreamId streamId, DomainEvent event, long version) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        new Version(version),
        new EventType("CustomerRegistered"),
        event,
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("corr-it"),
            null,
            null,
            Instant.now()));
  }
}
