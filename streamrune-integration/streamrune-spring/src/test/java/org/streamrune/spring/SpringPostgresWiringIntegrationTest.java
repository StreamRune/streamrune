package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.health.contributor.Status;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.integration.ReadModelStores;
import org.streamrune.postgres.PostgresEventStoreFactory;
import org.streamrune.postgres.PostgresSnapshotStorePurger;
import org.streamrune.runtime.gdpr.ForgetResult;
import org.streamrune.runtime.gdpr.ForgetSubjectService;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

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
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_spring_it");

  record CustomerRegistered(String customerId, @Encrypted(subjectId = "customerId") String email)
      implements DomainEvent {}

  /** A customer's state as a snapshot keeps it: the email in the clear, as a state often does. */
  record CustomerState(String customerId, String email) implements AggregateState {}

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
            .registerState("CustomerState", CustomerState.class)
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
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
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
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(DataSource.class, () -> dataSource)
        .withBean(EventTypeRegistry.class, () -> typeRegistry)
        .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
        .run(
            ctxA -> {
              var leaderA =
                  ctxA.getBean(org.streamrune.core.subscription.SubscriptionLeadership.class);
              assertThat(leaderA).isInstanceOf(org.streamrune.postgres.LeaseBasedLeadership.class);
              assertThat(leaderA.tryAcquire(SubscriptionName.of(consumer))).isPresent();
              assertThat(leaderA.current(SubscriptionName.of(consumer))).isPresent();

              // A second independent context (own auto-wired leadership bean, own holder id) must
              // be
              // standby for the same name while A still holds a live lease.
              new ApplicationContextRunner()
                  .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
                  .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
                  .withBean(DataSource.class, () -> dataSource)
                  .withBean(EventTypeRegistry.class, () -> typeRegistry)
                  .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
                  .run(
                      ctxB -> {
                        var leaderB =
                            ctxB.getBean(
                                org.streamrune.core.subscription.SubscriptionLeadership.class);
                        assertThat(leaderB.tryAcquire(SubscriptionName.of(consumer)))
                            .isEmpty(); // standby
                        assertThat(leaderB.current(SubscriptionName.of(consumer))).isEmpty();

                        // After A resigns (its lease expires), B can win the name — automatic
                        // failover, with a strictly greater fencing epoch than A held.
                        leaderA.resign(SubscriptionName.of(consumer));
                        assertThat(leaderB.tryAcquire(SubscriptionName.of(consumer))).isPresent();
                        leaderB.resign(SubscriptionName.of(consumer));
                      });
            });
  }

  /**
   * The auto-configured event store reads the saga rows of the auto-configured saga store in its
   * forward fence: both are built over the one {@code DataSource} bean, so the identities a {@code
   * SagaRunner} compares at {@code build()} are equal, and the command bus reports the same one.
   */
  @Test
  void theEventStoreFencesAgainstTheSagaStoresRows() {
    new ApplicationContextRunner()
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(DataSource.class, () -> dataSource)
        .withBean(EventTypeRegistry.class, () -> typeRegistry)
        .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
        .withPropertyValues("streamrune.saga.compensation-retry-enabled=false")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              EventStore eventStore = ctx.getBean(EventStore.class);
              SagaStore sagaStore = ctx.getBean(SagaStore.class);
              assertThat(eventStore.sagaFenceStorageIdentity()).isNotNull();
              assertThat(eventStore.sagaFenceStorageIdentity())
                  .isEqualTo(sagaStore.storageIdentity());
              assertThat(ctx.getBean(CommandBus.class).sagaFenceStorageIdentity())
                  .isEqualTo(sagaStore.storageIdentity());
            });
  }

  /**
   * The guarantee the snapshot-purge rule exists for, end to end: a {@code
   * PostgresSnapshotStorePurger} bean is run by the auto-configured {@code ForgetSubjectService},
   * and the subject's snapshot in {@code snapshot_store} — with the email in the clear — is gone
   * after the forget (its row keeps no state), while another customer's snapshot stays.
   */
  @Test
  void theAutoConfiguredForgetPurgesTheSubjectsSnapshot() {
    var subject = "snap-" + UUID.randomUUID();
    var customer = AggregateType.of("customer");
    var subjectStream = StreamId.of(customer, AggregateId.of(subject));
    var otherStream = StreamId.of(customer, AggregateId.of("other-" + UUID.randomUUID()));
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(DataSource.class, () -> dataSource)
        .withBean(EventTypeRegistry.class, () -> typeRegistry)
        .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
        .withBean(
            PostgresSnapshotStorePurger.class,
            () ->
                new PostgresSnapshotStorePurger(
                    dataSource,
                    subjectId -> List.of(StreamId.of(customer, AggregateId.of(subjectId.value())))))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              EventStore store = ctx.getBean(EventStore.class);
              store.saveSnapshot(
                  subjectStream, new Version(1), new CustomerState(subject, "alice@example.com"));
              store.saveSnapshot(
                  otherStream, new Version(1), new CustomerState("other", "bob@example.com"));
              assertThat(snapshotRows(subjectStream)).isEqualTo(1);

              ForgetResult result =
                  ctx.getBean(ForgetSubjectService.class)
                      .forget(SubjectId.of(subject), UserId.of("dpo"));

              assertThat(result.fullyErased()).isTrue();
              assertThat(result.purgeOutcomes())
                  .extracting(ForgetResult.PurgeOutcome::purgerName)
                  .containsExactly("snapshot_store");
              assertThat(snapshotRows(subjectStream)).isZero();
              assertThat(snapshotRows(otherStream)).isEqualTo(1);
            });
  }

  /**
   * The auto-configured routing on the default stores, end to end: a {@code TRANSACTIONAL_LOCAL}
   * {@link BaseProjection} over the injected {@code ProjectionRepository} (the default {@code
   * postgres} read-model store's repository) is routed to that store by its {@code writeTarget()},
   * so its checkpoint lands in {@code readmodel_checkpoint}; an {@code AT_LEAST_ONCE_IDEMPOTENT}
   * projection with no write target is routed to the event store's subscription checkpoint, so its
   * checkpoint lands in {@code subscription_checkpoint}. Neither projection has a row in the other
   * table.
   */
  @Test
  void
      transactionalProjectionCheckpointsInReadModelTable_atLeastOnceWithoutTargetInSubscriptionTable() {
    var streamId =
        StreamId.of(AggregateType.of("customer"), AggregateId.of("routed-" + UUID.randomUUID()));
    new ApplicationContextRunner()
        .withPropertyValues(
            SpringTestMocks.NO_SNAPSHOTS,
            "streamrune.projections.auto-discovery.enabled=true",
            "streamrune.polling-interval=50ms",
            "streamrune.polling-jitter=0s")
        .withConfiguration(
            AutoConfigurations.of(StreamRuneAutoConfiguration.class, ProjectionAutoConfig.class))
        .withBean(DataSource.class, () -> dataSource)
        .withBean(EventTypeRegistry.class, () -> typeRegistry)
        .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
        .withUserConfiguration(RoutedProjectionsConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              EventStore store = ctx.getBean(EventStore.class);
              long offset =
                  store
                      .append(
                          streamId,
                          List.of(
                              envelope(
                                  streamId, new CustomerRegistered("cust-r", "r@example.com"))),
                          Version.initial())
                      .globalOffsets()
                      .getLast()
                      .value();

              Awaitility.await()
                  .atMost(Duration.ofSeconds(20))
                  .untilAsserted(
                      () -> {
                        assertThat(checkpoint("readmodel_checkpoint", RoutedTransactional.NAME))
                            .as(
                                "the TRANSACTIONAL_LOCAL projection checkpoints with its read model")
                            .hasValueSatisfying(o -> assertThat(o).isGreaterThanOrEqualTo(offset));
                        assertThat(checkpoint("subscription_checkpoint", RoutedAtLeastOnce.NAME))
                            .as("the at-least-once projection checkpoints in the event store")
                            .hasValueSatisfying(o -> assertThat(o).isGreaterThanOrEqualTo(offset));
                      });
              assertThat(checkpoint("subscription_checkpoint", RoutedTransactional.NAME)).isEmpty();
              assertThat(checkpoint("readmodel_checkpoint", RoutedAtLeastOnce.NAME)).isEmpty();
              assertThat(
                      ctx.getBean(org.streamrune.core.projection.ProjectionRepository.class)
                          .findById(
                              org.streamrune.core.types.ProjectionName.of(RoutedTransactional.NAME),
                              "cust-r",
                              String.class))
                  .as("the TRANSACTIONAL_LOCAL read model is in the default store")
                  .contains("r@example.com");
              assertThat(ctx.getBean(RoutedAtLeastOnce.class).seen.get()).isPositive();
            });
  }

  /**
   * An application that keeps its subscription checkpoints on a second database — its own {@code
   * PostgresOffsetStore} over a DataSource that is not the application's {@code DataSource} bean —
   * gets a subscription checkpoint whose processor commits to that same second database: the batch
   * the processor commits is the offset the offset store reads, and nothing lands in the primary
   * database's {@code subscription_checkpoint}.
   */
  @Test
  void anApplicationOffsetStoreOnASecondDatabase_getsAProcessorOnThatDatabase() throws Exception {
    PGSimpleDataSource second = secondDatabase();
    var name =
        org.streamrune.core.types.ProjectionName.of(
            "second_db_" + UUID.randomUUID().toString().replace("-", ""));
    new ApplicationContextRunner()
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(DataSource.class, () -> dataSource)
        .withBean(EventTypeRegistry.class, () -> typeRegistry)
        .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
        .withBean(
            org.streamrune.core.projection.OffsetStore.class,
            () -> new org.streamrune.postgres.PostgresOffsetStore(second))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var pair =
                  ctx.getBean(
                      StreamRuneAutoConfiguration.SUBSCRIPTION_CHECKPOINT,
                      org.streamrune.core.projection.CheckpointTarget.class);
              assertThat(pair.checkpoints())
                  .isSameAs(ctx.getBean(org.streamrune.core.projection.OffsetStore.class));
              pair.processor()
                  .executeAtomically(
                      name,
                      List.of(),
                      GlobalOffset.of(7),
                      0L,
                      repository -> {},
                      pair.checkpoints());
              assertThat(pair.checkpoints().getLastOffset(name))
                  .as("the offset store reads what the processor committed")
                  .isEqualTo(GlobalOffset.of(7));
              assertThat(checkpoint("subscription_checkpoint", name.value()))
                  .as("nothing lands in the primary database")
                  .isEmpty();
            });
  }

  // ── several read-model stores: the default replaced only by name ─────────────────

  /**
   * An application store named {@code postgres} — here a Postgres store on a database of its own —
   * replaces the default store: it is the only store, the {@code ProjectionRepository} bean is its
   * repository, a projection over that repository is routed to it, and the default store is never
   * created: its read-model series never reaches the application's {@code DataSource}.
   */
  @Test
  void userStoreNamedPostgresReplacesTheDefault() throws Exception {
    PGSimpleDataSource fresh = secondDatabase();
    PGSimpleDataSource own = secondDatabase();
    var replacement =
        org.streamrune.postgres.PostgresReadModelStore.create(
            "postgres",
            own,
            org.streamrune.postgres.JdbcProjectionRepository.createObjectMapper(null),
            true,
            true);
    storesRunner(fresh)
        .withBean(
            "appPostgres", org.streamrune.core.projection.ReadModelStore.class, () -> replacement)
        .withUserConfiguration(TransactionalOverTheRepositoryBean.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(ReadModelStores.class).stores()).containsExactly(replacement);
              assertThat(ctx.getBean(org.streamrune.core.projection.ProjectionRepository.class))
                  .isSameAs(replacement.repository());
              var runner = ctx.getBean(org.streamrune.runtime.MultiProjectionRunner.class);
              assertThat(targetNameOf(runner, RoutedTransactional.NAME)).isEqualTo("postgres");
              assertThat(processorOf(runner, RoutedTransactional.NAME))
                  .isSameAs(replacement.processor());
              assertThat(tableExists(fresh, "readmodel_checkpoint"))
                  .as("the replaced default store is never created")
                  .isFalse();
            });
  }

  /**
   * An application store of another name is added beside the default store: both are routing
   * targets — a projection declaring the added store goes there, a projection over the {@code
   * ProjectionRepository} bean goes to the default store, which is created on the {@code
   * DataSource}.
   */
  @Test
  void additionalUserStoreKeepsTheDefault() throws Exception {
    PGSimpleDataSource fresh = secondDatabase();
    var search = new org.streamrune.test.InMemoryProjectionRepository();
    storesRunner(fresh)
        .withBean(
            "searchStore",
            org.streamrune.core.projection.ReadModelStore.class,
            () ->
                org.streamrune.core.projection.ReadModelStore.of("search", search, search, search))
        .withUserConfiguration(TransactionalOverTheRepositoryBean.class)
        .withBean(SearchCatalog.class, SearchCatalog::new)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(ReadModelStores.class).stores())
                  .extracting(org.streamrune.core.projection.ReadModelStore::name)
                  .containsExactly("postgres", "search");
              var runner = ctx.getBean(org.streamrune.runtime.MultiProjectionRunner.class);
              assertThat(targetNameOf(runner, SearchCatalog.NAME)).isEqualTo("search");
              assertThat(processorOf(runner, SearchCatalog.NAME)).isSameAs(search);
              assertThat(targetNameOf(runner, RoutedTransactional.NAME)).isEqualTo("postgres");
              assertThat(processorOf(runner, RoutedTransactional.NAME))
                  .isInstanceOf(org.streamrune.postgres.JdbcProjectionRepository.class);
              assertThat(tableExists(fresh, "readmodel_checkpoint"))
                  .as("the default store is created on the DataSource")
                  .isTrue();
            });
  }

  /**
   * Two stores with the same name fail the start as a store error, while the stores are collected —
   * before any registration is routed and before the default store is created.
   */
  @Test
  void duplicateStoreNamesFailStartup() throws Exception {
    PGSimpleDataSource fresh = secondDatabase();
    var first = new org.streamrune.test.InMemoryProjectionRepository();
    var second = new org.streamrune.test.InMemoryProjectionRepository();
    storesRunner(fresh)
        .withBean(
            "firstSearch",
            org.streamrune.core.projection.ReadModelStore.class,
            () -> org.streamrune.core.projection.ReadModelStore.of("search", first, first, first))
        .withBean(
            "secondSearch",
            org.streamrune.core.projection.ReadModelStore.class,
            () ->
                org.streamrune.core.projection.ReadModelStore.of("search", second, second, second))
        .withBean(SearchCatalog.class, SearchCatalog::new)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("Two read-model stores are named 'search'");
              assertThat(tableExists(fresh, "readmodel_checkpoint"))
                  .as("the default store is not created for an invalid store set")
                  .isFalse();
            });
  }

  /**
   * Each index is declared on the store its projection is routed to: the added store's repository
   * receives the routed projection's index and the default store does not; an index for a
   * projection with no registration goes to the default store, whose database gets it.
   */
  @Test
  void indexIsDeclaredOnTheRoutedStore() throws Exception {
    PGSimpleDataSource fresh = secondDatabase();
    var search = new org.streamrune.test.InMemoryProjectionRepository();
    var routedIndex =
        org.streamrune.core.projection.ProjectionIndex.forSort(
            org.streamrune.core.types.ProjectionName.of(SearchCatalog.NAME),
            org.streamrune.core.Sort.by("title"));
    var unregisteredIndex =
        org.streamrune.core.projection.ProjectionIndex.forSort(
            org.streamrune.core.types.ProjectionName.of("unregistered_view_model"),
            org.streamrune.core.Sort.by("title"));
    storesRunner(fresh)
        .withBean(
            "searchStore",
            org.streamrune.core.projection.ReadModelStore.class,
            () ->
                org.streamrune.core.projection.ReadModelStore.of("search", search, search, search))
        .withBean(SearchCatalog.class, SearchCatalog::new)
        .withBean(
            "routedIndex", org.streamrune.core.projection.ProjectionIndex.class, () -> routedIndex)
        .withBean(
            "unregisteredIndex",
            org.streamrune.core.projection.ProjectionIndex.class,
            () -> unregisteredIndex)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(search.declaredIndexes()).containsExactly(routedIndex);
              assertThat(indexCount(fresh, "unregistered_view_model_view"))
                  .as("the unregistered projection's index is on the default store")
                  .isPositive();
              assertThat(tableExists(fresh, SearchCatalog.NAME + "_view"))
                  .as("the routed projection's index never reaches the default store")
                  .isFalse();
            });
  }

  /**
   * With an added store beside it, the injectable {@code ProjectionRepository} bean stays the
   * default store's repository.
   */
  @Test
  void primaryProjectionRepositoryBeanIsTheDefaultStoresRepository() throws Exception {
    PGSimpleDataSource fresh = secondDatabase();
    var search = new org.streamrune.test.InMemoryProjectionRepository();
    storesRunner(fresh)
        .withBean(
            "searchStore",
            org.streamrune.core.projection.ReadModelStore.class,
            () ->
                org.streamrune.core.projection.ReadModelStore.of("search", search, search, search))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var defaultStore =
                  ctx.getBean(
                      org.streamrune.integration.ProjectionStoreRouting.DEFAULT_STORE_BEAN,
                      org.streamrune.core.projection.ReadModelStore.class);
              assertThat(defaultStore.name()).isEqualTo("postgres");
              assertThat(ctx.getBean(org.streamrune.core.projection.ProjectionRepository.class))
                  .isSameAs(defaultStore.repository())
                  .isNotSameAs(search);
            });
  }

  /** The auto-configuration with projection discovery, over {@code database}. */
  private static ApplicationContextRunner storesRunner(DataSource database) {
    return new ApplicationContextRunner()
        .withPropertyValues(
            SpringTestMocks.NO_SNAPSHOTS,
            "streamrune.projections.auto-discovery.enabled=true",
            "streamrune.polling-interval=50ms",
            "streamrune.polling-jitter=0s")
        .withConfiguration(
            AutoConfigurations.of(StreamRuneAutoConfiguration.class, ProjectionAutoConfig.class))
        .withBean(DataSource.class, () -> database)
        .withBean(EventTypeRegistry.class, () -> typeRegistry)
        .withBean(CryptoEngine.class, InMemoryCryptoEngine::new);
  }

  /** The transactional projection over the injected {@code ProjectionRepository} bean. */
  @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
  static class TransactionalOverTheRepositoryBean {
    @org.springframework.context.annotation.Bean
    RoutedTransactional routedTransactional(
        org.streamrune.core.projection.ProjectionRepository repository) {
      return new RoutedTransactional(repository);
    }
  }

  /** An at-least-once projection that declares the {@code search} store. */
  @ProjectionConfig(
      name = SearchCatalog.NAME,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT,
      readModelStore = "search")
  static class SearchCatalog implements Projection {
    static final String NAME = "search_catalog";

    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  private static Object processorOf(
      org.streamrune.runtime.MultiProjectionRunner runner, String projection) {
    return pairPart(runner, projection, "processor");
  }

  private static String targetNameOf(
      org.streamrune.runtime.MultiProjectionRunner runner, String projection) {
    return (String) pairPart(runner, projection, "targetName");
  }

  private static Object pairPart(
      org.streamrune.runtime.MultiProjectionRunner runner, String projection, String part) {
    var pairs =
        (java.util.Map<?, ?>)
            org.springframework.test.util.ReflectionTestUtils.getField(runner, "checkpoints");
    Object pair = pairs.get(org.streamrune.core.types.ProjectionName.of(projection));
    assertThat(pair).as("the routed pair of %s", projection).isNotNull();
    return org.springframework.test.util.ReflectionTestUtils.invokeMethod(pair, part);
  }

  private static boolean tableExists(DataSource database, String table) throws Exception {
    try (var conn = database.getConnection();
        var ps = conn.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
      ps.setString(1, table);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getBoolean(1);
      }
    }
  }

  /** The indexes on {@code table} besides its primary key: the declared ones. */
  private static int indexCount(DataSource database, String table) throws Exception {
    try (var conn = database.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT count(*) FROM pg_indexes WHERE tablename = ? AND indexname NOT LIKE"
                    + " '%_pkey'")) {
      ps.setString(1, table);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  /** A second database on the container, carrying the event-store series. */
  private static PGSimpleDataSource secondDatabase() throws Exception {
    String database = "second_" + UUID.randomUUID().toString().replace("-", "");
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE " + database);
    }
    var second = new PGSimpleDataSource();
    second.setUrl(PG.getJdbcUrl().replace("/streamrune_spring_it", "/" + database));
    second.setUser(PG.getUsername());
    second.setPassword(PG.getPassword());
    new PostgresEventStoreFactory(second, typeRegistry)
        .cryptoEngine(new InMemoryCryptoEngine())
        .create();
    return second;
  }

  /** The two projections of the routing test, over the injected default repository. */
  @org.springframework.context.annotation.Configuration(proxyBeanMethods = false)
  static class RoutedProjectionsConfig {
    @org.springframework.context.annotation.Bean
    RoutedTransactional routedTransactional(
        org.streamrune.core.projection.ProjectionRepository repository) {
      return new RoutedTransactional(repository);
    }

    @org.springframework.context.annotation.Bean
    RoutedAtLeastOnce routedAtLeastOnce() {
      return new RoutedAtLeastOnce();
    }
  }

  /** Writes each registration's email through the handed repository. */
  @ProjectionConfig(
      name = RoutedTransactional.NAME,
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class RoutedTransactional extends BaseProjection {
    static final String NAME = "routed_tl";

    RoutedTransactional(org.streamrune.core.projection.ProjectionRepository repository) {
      super(repository, NAME);
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      for (EventEnvelope envelope : batch) {
        if (envelope.event() instanceof CustomerRegistered registered) {
          save(registered.customerId(), registered.email());
        }
      }
    }
  }

  /** Counts the events it sees; writes nowhere the framework knows. */
  @ProjectionConfig(
      name = RoutedAtLeastOnce.NAME,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class RoutedAtLeastOnce implements Projection {
    static final String NAME = "routed_alo";
    final AtomicInteger seen = new AtomicInteger();

    @Override
    public void process(List<EventEnvelope> batch) {
      seen.addAndGet(batch.size());
    }
  }

  private static Optional<Long> checkpoint(String table, String projectionName) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT last_offset FROM " + table + " WHERE projection_name = ?")) {
      ps.setString(1, projectionName);
      try (var rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rs.getLong(1)) : Optional.empty();
      }
    }
  }

  private static int snapshotRows(StreamId streamId) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT count(*) FROM snapshot_store WHERE aggregate_type = ? AND aggregate_id = ?"
                    + " AND state_payload IS NOT NULL")) {
      ps.setString(1, streamId.aggregateType().value());
      ps.setString(2, streamId.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
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
            CorrelationId.of("corr-it"),
            null,
            null,
            Instant.now()));
  }
}
