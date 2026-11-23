package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendOptionException;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.backend.EventStoreOptions;
import org.streamrune.core.backend.OutboxOptions;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.InMemoryEventAuditStore;
import org.streamrune.sqlite.OtherBackendEventAuditStore;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.test.InMemoryOutboxStore;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * {@link PostgresBackend} builds today's Postgres classes, with the arguments the Spring, Quarkus
 * and Micronaut integrations pass them, lazily and once.
 */
@Testcontainers
class PostgresBackendTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("postgres_backend_test");

  record Mapped(String value) implements DomainEvent {}

  record Secret(String userId, @Encrypted(subjectId = "userId") String email)
      implements DomainEvent {}

  static final EventTypeRegistry REGISTRY =
      SimpleEventTypeRegistry.builder().registerEvent("Mapped", Mapped.class).build();

  static final OutboxEventMapper MAPPER =
      envelope ->
          List.of(
              OutboxEntry.pending(
                  OutboxEntryId.of(envelope.metadata().eventId().value()),
                  "{\"k\":1}",
                  "Mapped",
                  envelope.streamId()));

  static PGSimpleDataSource dataSource;

  private final List<BackendStores> opened = new ArrayList<>();

  @BeforeAll
  static void dataSource() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    // The schema every store reads and writes, as an integration's event store creates it.
    new PostgresEventStoreFactory(dataSource, REGISTRY).create();
  }

  @AfterEach
  void closeBundles() {
    opened.forEach(BackendStores::close);
  }

  private BackendStores open(DataSource ds) {
    return open(ds, null);
  }

  private BackendStores open(DataSource ds, CryptoEngine cryptoEngine) {
    BackendStores stores = new PostgresBackend().open(context(ds, cryptoEngine));
    opened.add(stores);
    return stores;
  }

  private static BackendContext context(DataSource ds, CryptoEngine cryptoEngine) {
    return new BackendContext(
        ds,
        cryptoEngine,
        Clock.systemUTC(),
        StreamRuneMetrics.NOOP,
        StreamRuneTracing.NOOP,
        true,
        true,
        true);
  }

  private static EventStoreOptions options() {
    return options(null, null, null, null, null);
  }

  private static EventStoreOptions options(
      OutboxStore outbox,
      OutboxEventMapper mapper,
      org.streamrune.core.CommandInbox inbox,
      org.streamrune.core.audit.EventAuditStore audit,
      Duration statementTimeout) {
    return new EventStoreOptions(
        REGISTRY, List.of(), List.of(), outbox, mapper, inbox, audit, statementTimeout);
  }

  // ---------------------------------------------------------------- naming, opening

  @Test
  void theBackendIsNamedPostgres() {
    assertThat(new PostgresBackend().name()).isEqualTo("postgres");
    assertThat(open(dataSource).backendName()).isEqualTo("postgres");
  }

  @Test
  void openNeedsAContext() {
    assertThatThrownBy(() -> new PostgresBackend().open(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("context is required");
  }

  @Test
  void openingAndClosingABundleRunsNoSql() {
    CountingDataSource counting = new CountingDataSource(dataSource);
    BackendStores stores = open(counting);
    stores.close();
    assertThat(counting.connections()).isZero();
  }

  @Test
  void buildingTheStoresTouchesNoTableExceptTheDefaultReadModelStore() {
    CountingDataSource counting = new CountingDataSource(dataSource);
    BackendStores stores = open(counting);
    stores.eventStoreFactory(options());
    stores.offsetStore();
    stores.sagaStore();
    stores.sagaDeadLetterStore();
    stores.outboxStore(
        new OutboxOptions(Duration.ofMinutes(4), OutboxOrderingMode.STRICT_PER_AGGREGATE));
    stores.commandInbox();
    stores.deadLetterQueue();
    stores.projectionDeadLetterStore();
    stores.auditStore();
    stores.eventAuditStore();
    stores.commandAuditQuery();
    stores.eventAuditQuery();
    stores.complianceReportQuery();
    stores.snapshotPurger(subject -> List.of());
    assertThat(counting.connections()).isZero();
  }

  // ---------------------------------------------------------------- what each accessor builds

  @Test
  void eachAccessorBuildsTodaysPostgresClass() {
    BackendStores stores = open(dataSource);
    assertThat(stores.eventStoreFactory(options()))
        .isExactlyInstanceOf(PostgresEventStoreFactory.class);
    assertThat(stores.offsetStore()).isExactlyInstanceOf(PostgresOffsetStore.class);
    assertThat(stores.defaultReadModelStore()).isExactlyInstanceOf(PostgresReadModelStore.class);
    assertThat(stores.sagaStore()).isExactlyInstanceOf(PostgresSagaStore.class);
    assertThat(stores.sagaDeadLetterStore()).isExactlyInstanceOf(PostgresSagaDeadLetterStore.class);
    assertThat(
            stores.outboxStore(
                new OutboxOptions(Duration.ofMinutes(1), OutboxOrderingMode.AVAILABILITY_FIRST)))
        .isExactlyInstanceOf(PostgresOutboxStore.class);
    assertThat(stores.commandInbox()).isExactlyInstanceOf(PostgresCommandInbox.class);
    assertThat(stores.deadLetterQueue()).isExactlyInstanceOf(PostgresDeadLetterQueue.class);
    assertThat(stores.projectionDeadLetterStore())
        .isExactlyInstanceOf(PostgresProjectionDeadLetterStore.class);
    assertThat(stores.auditStore()).isExactlyInstanceOf(PostgresAuditStore.class);
    assertThat(stores.eventAuditStore()).isExactlyInstanceOf(PostgresEventAuditStore.class);
    assertThat(stores.commandAuditQuery()).isExactlyInstanceOf(PostgresCommandAuditQuery.class);
    assertThat(stores.eventAuditQuery()).isExactlyInstanceOf(PostgresEventAuditQuery.class);
    assertThat(stores.complianceReportQuery())
        .isExactlyInstanceOf(PostgresComplianceReportQuery.class);
    assertThat(stores.snapshotPurger(subject -> List.of()))
        .isExactlyInstanceOf(PostgresSnapshotStorePurger.class);
    assertThat(stores.leadership(Duration.ofSeconds(15)))
        .isExactlyInstanceOf(LeaseBasedLeadership.class);
  }

  @Test
  void theDefaultReadModelStoreIsNamedAfterTheBackend() {
    ReadModelStore store = open(dataSource).defaultReadModelStore();
    assertThat(store.name()).isEqualTo("postgres").isNotEqualTo(ReadModelStore.DEFAULT);
  }

  @Test
  void theOutboxStoreCarriesTheOptions() {
    OutboxStore store =
        open(dataSource)
            .outboxStore(
                new OutboxOptions(Duration.ofSeconds(90), OutboxOrderingMode.AVAILABILITY_FIRST));
    assertThat(store.claimLease()).isEqualTo(Duration.ofSeconds(90));
    assertThat(store.orderingMode()).isEqualTo(OutboxOrderingMode.AVAILABILITY_FIRST);
  }

  @Test
  void anOutboxStoreBuiltWithoutALeaseUsesTheSpiDefault() {
    assertThat(new PostgresOutboxStore(dataSource).claimLease())
        .isEqualTo(OutboxOptions.DEFAULT_CLAIM_LEASE);
  }

  // ---------------------------------------------------------------- the two mappers

  @Test
  void theSagaAndReadModelMappersAreBuiltAsTheIntegrationsBuildThem() throws Exception {
    BackendStores stores = open(dataSource, new InMemoryCryptoEngine());
    ObjectMapper saga = objectMapperOf(stores.sagaStore());
    ObjectMapper readModel = objectMapperOf(stores.defaultReadModelStore().repository());

    // PostgresSagaStore.createObjectMapper keeps timestamps numeric; the read-model mapper writes
    // ISO-8601 text. Both carry the context's crypto engine.
    assertThat(saga.isEnabled(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)).isTrue();
    assertThat(readModel.isEnabled(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)).isFalse();
    assertThat(saga.getRegisteredModuleIds()).contains("CryptoShreddingModule");
    assertThat(readModel.getRegisteredModuleIds()).contains("CryptoShreddingModule");
  }

  @Test
  void withoutACryptoEngineTheMappersAreCryptoBlind() throws Exception {
    BackendStores stores = open(dataSource);
    assertThat(objectMapperOf(stores.sagaStore()).getRegisteredModuleIds())
        .doesNotContain("CryptoShreddingModule");
    assertThat(objectMapperOf(stores.defaultReadModelStore().repository()).getRegisteredModuleIds())
        .doesNotContain("CryptoShreddingModule");
  }

  private static ObjectMapper objectMapperOf(Object store) throws ReflectiveOperationException {
    Field field = store.getClass().getDeclaredField("objectMapper");
    field.setAccessible(true);
    return (ObjectMapper) field.get(store);
  }

  // ---------------------------------------------------------------- memoization

  @Test
  void repeatedCallsReturnTheSameInstance() {
    BackendStores stores = open(dataSource);
    assertThat(stores.offsetStore()).isSameAs(stores.offsetStore());
    assertThat(stores.defaultReadModelStore()).isSameAs(stores.defaultReadModelStore());
    assertThat(stores.sagaStore()).isSameAs(stores.sagaStore());
    assertThat(stores.sagaDeadLetterStore()).isSameAs(stores.sagaDeadLetterStore());
    assertThat(stores.commandInbox()).isSameAs(stores.commandInbox());
    assertThat(stores.deadLetterQueue()).isSameAs(stores.deadLetterQueue());
    assertThat(stores.projectionDeadLetterStore()).isSameAs(stores.projectionDeadLetterStore());
    assertThat(stores.auditStore()).isSameAs(stores.auditStore());
    assertThat(stores.eventAuditStore()).isSameAs(stores.eventAuditStore());
    assertThat(stores.commandAuditQuery()).isSameAs(stores.commandAuditQuery());
    assertThat(stores.eventAuditQuery()).isSameAs(stores.eventAuditQuery());
    assertThat(stores.complianceReportQuery()).isSameAs(stores.complianceReportQuery());
    assertThat(stores.eventStoreFactory(options())).isSameAs(stores.eventStoreFactory(options()));
    assertThat(
            stores.outboxStore(
                new OutboxOptions(Duration.ofMinutes(4), OutboxOrderingMode.STRICT_PER_AGGREGATE)))
        .isSameAs(
            stores.outboxStore(
                new OutboxOptions(Duration.ofMinutes(4), OutboxOrderingMode.STRICT_PER_AGGREGATE)));
    assertThat(stores.leadership(Duration.ofSeconds(15)))
        .isSameAs(stores.leadership(Duration.ofMillis(15_000)));
  }

  @Test
  void differentArgumentsBuildDifferentInstances() {
    BackendStores stores = open(dataSource);
    assertThat(stores.eventStoreFactory(options()))
        .isNotSameAs(stores.eventStoreFactory(options(null, null, null, null, Duration.ZERO)));
    assertThat(
            stores.outboxStore(
                new OutboxOptions(Duration.ofMinutes(4), OutboxOrderingMode.STRICT_PER_AGGREGATE)))
        .isNotSameAs(
            stores.outboxStore(
                new OutboxOptions(Duration.ofMinutes(4), OutboxOrderingMode.AVAILABILITY_FIRST)));
    assertThat(stores.leadership(Duration.ofSeconds(15)))
        .isNotSameAs(stores.leadership(Duration.ofSeconds(16)));
  }

  @Test
  void theSnapshotPurgerIsNewOnEveryCall() {
    BackendStores stores = open(dataSource);
    assertThat(stores.snapshotPurger(subject -> List.of()))
        .isNotSameAs(stores.snapshotPurger(subject -> List.of()));
  }

  @Test
  void nullArgumentsAreRefused() {
    BackendStores stores = open(dataSource);
    assertThatThrownBy(() -> stores.eventStoreFactory(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("options is required");
    assertThatThrownBy(() -> stores.outboxStore(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("options is required");
    assertThatThrownBy(() -> stores.snapshotPurger(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("streamsForSubject is required");
    assertThatThrownBy(() -> stores.leadership(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("leaseTtl must be a positive duration");
  }

  @Test
  void aLeaseTtlLeaseBasedLeadershipRefusesIsRefused() {
    BackendStores stores = open(dataSource);
    assertThatThrownBy(() -> stores.leadership(Duration.ofNanos(500_000)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("leaseTtl must be at least 1ms");
  }

  // ---------------------------------------------------------------- the event store factory

  @Test
  void aForeignOutboxIsRefusedWithTheR1MessageWithoutTouchingTheDatabase() {
    CountingDataSource counting = new CountingDataSource(dataSource);
    BackendStores stores = open(counting);
    assertThatThrownBy(
            () ->
                stores.eventStoreFactory(
                    options(new InMemoryOutboxStore(), MAPPER, null, null, null)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "outbox store of backend 'InMemoryOutboxStore' cannot share the 'postgres' event"
                + " store's transaction; build it from this backend's BackendStores");
    assertThat(counting.connections()).isZero();
  }

  @Test
  void aForeignCommandInboxIsRefusedWithTheR1Message() {
    BackendStores stores = open(dataSource);
    assertThatThrownBy(
            () ->
                stores.eventStoreFactory(
                    options(null, null, new InMemoryCommandInbox(), null, null)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "command inbox store of backend 'InMemoryCommandInbox' cannot share the 'postgres'"
                + " event store's transaction; build it from this backend's BackendStores");
  }

  @Test
  void aForeignEventAuditStoreIsRefusedWithTheR1Message() {
    BackendStores stores = open(dataSource);
    assertThatThrownBy(
            () ->
                stores.eventStoreFactory(
                    options(null, null, null, new InMemoryEventAuditStore(), null)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "event audit store of backend 'InMemoryEventAuditStore' cannot share the 'postgres'"
                + " event store's transaction; build it from this backend's BackendStores");
  }

  @Test
  void theRefusalNamesTheBackendThatOwnsTheStore() {
    BackendStores stores = open(dataSource);
    assertThatThrownBy(
            () ->
                stores.eventStoreFactory(
                    options(null, null, null, new OtherBackendEventAuditStore(), null)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "event audit store of backend 'sqlite' cannot share the 'postgres' event store's"
                + " transaction; build it from this backend's BackendStores");
  }

  @Test
  void anAnonymousForeignStoreIsNamedByItsClassName() {
    BackendStores stores = open(dataSource);
    var anonymous =
        new org.streamrune.core.audit.EventAuditStore() {
          @Override
          public void save(org.streamrune.core.audit.EventAuditEntry entry) {}

          @Override
          public void saveAll(List<org.streamrune.core.audit.EventAuditEntry> entries) {}
        };
    assertThatThrownBy(() -> stores.eventStoreFactory(options(null, null, null, anonymous, null)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "event audit store of backend '" + anonymous.getClass().getName() + "' cannot share");
  }

  @Test
  void aStatementTimeoutPostgresCannotApplyIsABackendOptionException() {
    CountingDataSource counting = new CountingDataSource(dataSource);
    BackendStores stores = open(counting);
    Duration subMillisecond = Duration.ofNanos(500_000);
    IllegalArgumentException original =
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new PostgresEventStoreFactory(dataSource, REGISTRY)
                    .statementTimeout(subMillisecond));

    assertThatThrownBy(
            () -> stores.eventStoreFactory(options(null, null, null, null, subMillisecond)))
        .isExactlyInstanceOf(BackendOptionException.class)
        .hasMessage(original.getMessage())
        .hasCauseExactlyInstanceOf(IllegalArgumentException.class)
        .satisfies(
            e -> assertThat(((BackendOptionException) e).option()).isEqualTo("statementTimeout"));
    assertThatThrownBy(
            () -> stores.eventStoreFactory(options(null, null, null, null, Duration.ofSeconds(-1))))
        .isExactlyInstanceOf(BackendOptionException.class)
        .hasMessageContaining("non-negative");
    assertThat(counting.connections()).isZero();
  }

  @Test
  void aZeroStatementTimeoutIsAccepted() {
    BackendStores stores = open(dataSource);
    assertThatCode(() -> stores.eventStoreFactory(options(null, null, null, null, Duration.ZERO)))
        .doesNotThrowAnyException();
  }

  @Test
  void userDeclaredPostgresStoresAreAcceptedAndWrittenInTheAppend() {
    BackendStores stores = open(dataSource);
    var outbox = new PostgresOutboxStore(dataSource, Duration.ofMinutes(1));
    EventStoreFactory factory =
        stores.eventStoreFactory(
            options(
                outbox,
                MAPPER,
                new PostgresCommandInbox(dataSource),
                new PostgresEventAuditStore(dataSource),
                Duration.ofSeconds(5)));

    StreamId stream = TestStreams.stream("backend-outbox-" + System.nanoTime());
    factory.create().append(stream, List.of(envelope(stream)), Version.initial());

    assertThat(outbox.loadPending(100))
        .anySatisfy(entry -> assertThat(entry.streamId()).isEqualTo(stream));
  }

  @Test
  void storesFromTheBundleAreAcceptedToo() {
    BackendStores stores = open(dataSource);
    assertThatCode(
            () ->
                stores.eventStoreFactory(
                    options(
                        stores.outboxStore(
                            new OutboxOptions(
                                OutboxOptions.DEFAULT_CLAIM_LEASE,
                                OutboxOrderingMode.STRICT_PER_AGGREGATE)),
                        MAPPER,
                        stores.commandInbox(),
                        stores.eventAuditStore(),
                        null)))
        .doesNotThrowAnyException();
  }

  @Test
  void anOutboxStoreWithoutAMapperConfiguresNoOutbox() {
    BackendStores stores = open(dataSource);
    var outbox = new PostgresOutboxStore(dataSource, Duration.ofMinutes(1));
    EventStoreFactory factory = stores.eventStoreFactory(options(outbox, null, null, null, null));

    StreamId stream = TestStreams.stream("backend-no-mapper-" + System.nanoTime());
    factory.create().append(stream, List.of(envelope(stream)), Version.initial());

    assertThat(outbox.loadPending(100))
        .noneSatisfy(e -> assertThat(e.streamId()).isEqualTo(stream));
  }

  @Test
  void aMapperWithoutAnOutboxStoreConfiguresNoOutbox() {
    BackendStores stores = open(dataSource);
    EventStoreFactory factory = stores.eventStoreFactory(options(null, MAPPER, null, null, null));

    StreamId stream = TestStreams.stream("backend-no-store-" + System.nanoTime());
    // With the mapper alone the store would refuse an append whose mapper yields entries.
    assertThatCode(
            () -> factory.create().append(stream, List.of(envelope(stream)), Version.initial()))
        .doesNotThrowAnyException();
  }

  @Test
  void everyCreateOfTheMemoizedFactoryBuildsANewEventStore() {
    EventStoreFactory factory = open(dataSource).eventStoreFactory(options());
    EventStore first = factory.create();
    EventStore second = factory.create();
    assertThat(first).isExactlyInstanceOf(PostgresEventStore.class);
    assertThat(second).isExactlyInstanceOf(PostgresEventStore.class).isNotSameAs(first);
  }

  @Test
  void theEventStoreTakesTheContextsCryptoEngine() {
    EventStoreOptions secretOptions =
        new EventStoreOptions(
            SimpleEventTypeRegistry.builder().registerEvent("Secret", Secret.class).build(),
            List.of(),
            List.of(),
            null,
            null,
            null,
            null,
            null);
    // Without an engine, create() refuses an @Encrypted type before it touches the database.
    CountingDataSource counting = new CountingDataSource(dataSource);
    EventStoreFactory blind = open(counting).eventStoreFactory(secretOptions);
    assertThatThrownBy(blind::create).isInstanceOf(IllegalStateException.class);
    assertThat(counting.connections()).isZero();

    EventStoreFactory withEngine =
        open(dataSource, new InMemoryCryptoEngine()).eventStoreFactory(secretOptions);
    assertThat(withEngine.create()).isExactlyInstanceOf(PostgresEventStore.class);
  }

  private static EventEnvelope envelope(StreamId stream) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        stream,
        new Version(1),
        new EventType("Mapped"),
        new Mapped("v"),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            CorrelationId.of("backend-test"),
            null,
            null,
            Instant.now()));
  }

  // ---------------------------------------------------------------- close

  @Test
  void closeClosesTheLeadershipAndIsIdempotent() {
    BackendStores stores = open(dataSource);
    SubscriptionLeadership leadership = stores.leadership(Duration.ofSeconds(15));
    SubscriptionName name = SubscriptionName.of("backend-close-" + System.nanoTime());
    assertThat(leadership.tryAcquire(name)).isPresent();

    stores.close();

    assertThat(leadership.tryAcquire(name)).isEmpty();
    assertThatCode(stores::close).doesNotThrowAnyException();
  }

  @Test
  void aLeadershipAskedForAfterCloseIsClosedToo() {
    BackendStores stores = open(dataSource);
    SubscriptionLeadership before = stores.leadership(Duration.ofSeconds(15));
    stores.close();

    assertThat(stores.leadership(Duration.ofSeconds(15))).isSameAs(before);
    SubscriptionLeadership after = stores.leadership(Duration.ofSeconds(20));
    assertThat(after.tryAcquire(SubscriptionName.of("backend-after-close-" + System.nanoTime())))
        .isEmpty();
  }

  // ---------------------------------------------------------------- verify

  @Test
  void verifyAcceptsThePostgresServerUnderTest() {
    assertThatCode(() -> new PostgresBackend().verify(dataSource)).doesNotThrowAnyException();
  }

  @Test
  void verifyRefusesAnotherProductNamingBoth() throws SQLException {
    assertThatThrownBy(() -> new PostgresBackend().verify(server("H2", 2, "2.3.232 (2024-08-11)")))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("postgres")
        .hasMessageContaining("H2");
    assertThatThrownBy(() -> new PostgresBackend().verify(server("SQLite", 3, "3.50.3")))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("postgres")
        .hasMessageContaining("SQLite");
  }

  @Test
  void verifyRefusesAnOldPostgresWithItsOwnException() throws SQLException {
    assertThatThrownBy(() -> new PostgresBackend().verify(server("PostgreSQL", 16, "16.4")))
        .isExactlyInstanceOf(UnsupportedServerVersionException.class)
        .hasMessageContaining("16.4");
  }

  @Test
  void verifyReportsAnUnreachableDatabaseAsAnEventStoreException() throws SQLException {
    DataSource unreachable = mock(DataSource.class);
    when(unreachable.getConnection()).thenThrow(new SQLException("Connection refused"));
    assertThatThrownBy(() -> new PostgresBackend().verify(unreachable))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("Cannot read the PostgreSQL server version")
        .hasCauseExactlyInstanceOf(SQLException.class);
  }

  @Test
  void verifyNeedsADataSource() {
    assertThatThrownBy(() -> new PostgresBackend().verify(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("dataSource is required");
  }

  private static DataSource server(String product, int major, String version) throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData meta = mock(DatabaseMetaData.class);
    when(ds.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(meta);
    when(meta.getDatabaseProductName()).thenReturn(product);
    when(meta.getDatabaseMajorVersion()).thenReturn(major);
    when(meta.getDatabaseProductVersion()).thenReturn(version);
    return ds;
  }

  // ---------------------------------------------------------------- test DataSources

  /** Counts every connection checkout; delegates everything to the wrapped DataSource. */
  static final class CountingDataSource implements DataSource {
    private final DataSource delegate;
    private final AtomicInteger connections = new AtomicInteger();

    CountingDataSource(DataSource delegate) {
      this.delegate = delegate;
    }

    int connections() {
      return connections.get();
    }

    @Override
    public Connection getConnection() throws SQLException {
      connections.incrementAndGet();
      return delegate.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      connections.incrementAndGet();
      return delegate.getConnection(username, password);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
      delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
      delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      return delegate.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() {
      return Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return delegate.isWrapperFor(iface);
    }
  }
}
