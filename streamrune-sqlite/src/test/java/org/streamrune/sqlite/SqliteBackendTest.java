package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.CommandInbox;
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
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.audit.EventAuditStore;
import org.streamrune.core.backend.BackendContext;
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
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.postgres.PostgresEventAuditStore;
import org.streamrune.runtime.InMemoryEventAuditStore;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.test.InMemoryOutboxStore;

/**
 * {@link SqliteBackend} bundles the SQLite stores on one {@link SqliteDatabase} the way {@code
 * PostgresBackend} bundles the Postgres ones: lazily, once, and narrowing the event store's outbox,
 * inbox and audit store to this backend's own types.
 */
class SqliteBackendTest {

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

  private static final OutboxOptions STRICT =
      new OutboxOptions(OutboxOptions.DEFAULT_CLAIM_LEASE, OutboxOrderingMode.STRICT_PER_AGGREGATE);

  @TempDir Path dir;

  private final List<AutoCloseable> opened = new ArrayList<>();

  @AfterEach
  void closeAll() throws Exception {
    for (AutoCloseable resource : opened.reversed()) {
      resource.close();
    }
  }

  // ---------------------------------------------------------------- naming, opening

  @Test
  void theBackendIsNamedSqlite() {
    assertThat(new SqliteBackend().name()).isEqualTo("sqlite");
    assertThat(open("name.db").backendName()).isEqualTo("sqlite");
  }

  @Test
  void openNeedsAContext() {
    assertThatThrownBy(() -> new SqliteBackend().open(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("context is required");
  }

  /**
   * Opening borrows exactly one connection, the setup verification's, and takes the instance guard;
   * it writes nothing, so the file holds no table. Closing releases the guard.
   */
  @Test
  void openRunsOnlyTheSetupVerificationAndTakesTheGuard() throws Exception {
    Path file = dir.resolve("open.db");
    CountingDataSource counting = new CountingDataSource(TestDatabases.dataSource(file));
    BackendStores stores = new SqliteBackend().open(context(counting, null));
    try {
      assertThat(counting.connections()).isEqualTo(1);
      assertThat(tableCount(file)).isZero();
      assertThat(ForkedJvm.probe(file)).startsWith(GuardChild.REFUSED);
    } finally {
      stores.close();
    }
    assertThat(ForkedJvm.probe(file)).startsWith(GuardChild.ACQUIRED);
  }

  @Test
  void openRefusesADatabaseTheSetupVerificationRefuses() {
    DataSource walOff = TestDatabases.dataSource(dir.resolve("wal-off.db"), "journal_mode=DELETE");
    assertThatThrownBy(() -> new SqliteBackend().open(context(walOff, null)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("journal_mode");
  }

  @Test
  void buildingTheStoresTouchesNoTableExceptTheDefaultReadModelStore() throws Exception {
    Path file = dir.resolve("lazy.db");
    CountingDataSource counting = new CountingDataSource(TestDatabases.dataSource(file));
    BackendStores stores = open(counting, null);
    stores.eventStoreFactory(options());
    stores.offsetStore();
    stores.sagaStore();
    stores.sagaDeadLetterStore();
    stores.outboxStore(STRICT);
    stores.commandInbox();
    stores.deadLetterQueue();
    stores.projectionDeadLetterStore();
    stores.auditStore();
    stores.eventAuditStore();
    stores.commandAuditQuery();
    stores.eventAuditQuery();
    stores.complianceReportQuery();
    stores.snapshotPurger(subject -> List.of());
    stores.leadership(Duration.ofSeconds(15));
    // The one connection is open()'s setup verification.
    assertThat(counting.connections()).isEqualTo(1);
    assertThat(tableCount(file)).isZero();
  }

  // ---------------------------------------------------------------- what each accessor builds

  @Test
  void eachAccessorReturnsItsSqliteType() {
    BackendStores stores = migrated("types.db");
    assertThat(stores.eventStoreFactory(options()))
        .isExactlyInstanceOf(SqliteEventStoreFactory.class);
    assertThat(stores.offsetStore()).isExactlyInstanceOf(SqliteOffsetStore.class);
    assertThat(stores.defaultReadModelStore()).isExactlyInstanceOf(SqliteReadModelStore.class);
    assertThat(stores.sagaStore()).isExactlyInstanceOf(SqliteSagaStore.class);
    assertThat(stores.sagaDeadLetterStore()).isExactlyInstanceOf(SqliteSagaDeadLetterStore.class);
    assertThat(stores.outboxStore(STRICT)).isExactlyInstanceOf(SqliteOutboxStore.class);
    assertThat(stores.commandInbox()).isExactlyInstanceOf(SqliteCommandInbox.class);
    assertThat(stores.deadLetterQueue()).isExactlyInstanceOf(SqliteDeadLetterQueue.class);
    assertThat(stores.projectionDeadLetterStore())
        .isExactlyInstanceOf(SqliteProjectionDeadLetterStore.class);
    assertThat(stores.auditStore()).isExactlyInstanceOf(SqliteAuditStore.class);
    assertThat(stores.eventAuditStore()).isExactlyInstanceOf(SqliteEventAuditStore.class);
    assertThat(stores.commandAuditQuery()).isExactlyInstanceOf(SqliteCommandAuditQuery.class);
    assertThat(stores.eventAuditQuery()).isExactlyInstanceOf(SqliteEventAuditQuery.class);
    assertThat(stores.complianceReportQuery())
        .isExactlyInstanceOf(SqliteComplianceReportQuery.class);
    assertThat(stores.snapshotPurger(subject -> List.of()))
        .isExactlyInstanceOf(SqliteSnapshotStorePurger.class);
    assertThat(stores.leadership(Duration.ofSeconds(15))).isSameAs(SubscriptionLeadership.NOOP);
  }

  @Test
  void theDefaultReadModelStoreIsNamedAfterTheBackend() {
    ReadModelStore store = open("named.db").defaultReadModelStore();
    assertThat(store.name()).isEqualTo("sqlite").isNotEqualTo(ReadModelStore.DEFAULT);
  }

  /** The default read-model store applies the read-model series when it is first built. */
  @Test
  void theDefaultReadModelStoreMigratesItsSeriesWhenFirstBuilt() throws Exception {
    Path file = dir.resolve("readmodel.db");
    BackendStores stores = open(TestDatabases.dataSource(file), null);
    stores.defaultReadModelStore();
    assertThat(tableExists(file, "readmodel_checkpoint")).isTrue();
    assertThat(tableExists(file, SqliteSchemaSeries.READ_MODEL.historyTable())).isTrue();
    assertThat(tableExists(file, "event_stream")).isFalse();
  }

  /** The context's switches drive the read-model store: validation without migration fails. */
  @Test
  void theReadModelSwitchesComeFromTheContext() throws Exception {
    Path file = dir.resolve("readmodel-validate.db");
    BackendStores stores =
        openWith(
            new BackendContext(
                TestDatabases.dataSource(file),
                null,
                Clock.systemUTC(),
                StreamRuneMetrics.NOOP,
                StreamRuneTracing.NOOP,
                true,
                false,
                true));
    assertThatThrownBy(stores::defaultReadModelStore)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("readmodel_checkpoint");
    assertThat(tableCount(file)).isZero();
  }

  /** The event store's switches come from the context: validation without migration fails. */
  @Test
  void theEventStoreSwitchesComeFromTheContext() throws Exception {
    Path file = dir.resolve("eventstore-validate.db");
    BackendStores stores =
        openWith(
            new BackendContext(
                TestDatabases.dataSource(file),
                null,
                Clock.systemUTC(),
                StreamRuneMetrics.NOOP,
                StreamRuneTracing.NOOP,
                false,
                true,
                true));
    EventStoreFactory factory = stores.eventStoreFactory(options());
    assertThatThrownBy(factory::create)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("event_stream");
    assertThat(tableCount(file)).isZero();
  }

  @Test
  void theOutboxStoreCarriesTheOptions() {
    OutboxStore store =
        open("outbox.db")
            .outboxStore(
                new OutboxOptions(Duration.ofSeconds(90), OutboxOrderingMode.AVAILABILITY_FIRST));
    assertThat(store.claimLease()).isEqualTo(Duration.ofSeconds(90));
    assertThat(store.orderingMode()).isEqualTo(OutboxOrderingMode.AVAILABILITY_FIRST);
  }

  /**
   * Every store uses the context's own DataSource, and the saga store keeps the handle's file: what
   * a store retains of the bundle's one handle (the stores keep no handle, so handle identity
   * itself is the wiring's, by construction). The saga store and the saga dead-letter store matter
   * most: their shield and hold rely on sharing one database file.
   */
  @Test
  void everyStoreUsesTheContextsDataSourceAndTheSagaStoreItsFile() throws Exception {
    DataSource dataSource = TestDatabases.dataSource(dir.resolve("one-handle.db"));
    BackendStores stores = open(dataSource, null);
    for (Object store :
        List.of(
            stores.sagaStore(),
            stores.sagaDeadLetterStore(),
            stores.offsetStore(),
            stores.outboxStore(STRICT),
            stores.commandInbox(),
            stores.deadLetterQueue(),
            stores.projectionDeadLetterStore(),
            stores.auditStore(),
            stores.eventAuditStore(),
            stores.commandAuditQuery(),
            stores.eventAuditQuery(),
            stores.complianceReportQuery(),
            stores.defaultReadModelStore().repository())) {
      assertThat(field(store, "dataSource"))
          .as(store.getClass().getSimpleName())
          .isSameAs(dataSource);
    }
    assertThat(field(stores.sagaStore(), "file"))
        .isEqualTo(dir.resolve("one-handle.db").toRealPath());
  }

  // ---------------------------------------------------------------- the two mappers

  @Test
  void theSagaAndReadModelMappersAreBuiltAsOnPostgres() throws Exception {
    BackendStores stores = open(TestDatabases.dataSource(dir.resolve("mappers.db")), engine());
    ObjectMapper saga = (ObjectMapper) field(stores.sagaStore(), "objectMapper");
    ObjectMapper readModel =
        (ObjectMapper) field(stores.defaultReadModelStore().repository(), "objectMapper");

    assertThat(saga.isEnabled(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)).isTrue();
    assertThat(readModel.isEnabled(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)).isFalse();
    assertThat(saga.getRegisteredModuleIds()).contains("CryptoShreddingModule");
    assertThat(readModel.getRegisteredModuleIds()).contains("CryptoShreddingModule");
  }

  @Test
  void withoutACryptoEngineTheMappersAreCryptoBlind() throws Exception {
    BackendStores stores = open("blind.db");
    assertThat(((ObjectMapper) field(stores.sagaStore(), "objectMapper")).getRegisteredModuleIds())
        .doesNotContain("CryptoShreddingModule");
    assertThat(
            ((ObjectMapper) field(stores.defaultReadModelStore().repository(), "objectMapper"))
                .getRegisteredModuleIds())
        .doesNotContain("CryptoShreddingModule");
  }

  // ---------------------------------------------------------------- memoization

  @Test
  void repeatedCallsReturnTheSameInstance() {
    BackendStores stores = open("memo.db");
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
        .isSameAs(stores.outboxStore(STRICT));
    assertThat(stores.leadership(Duration.ofSeconds(15)))
        .isSameAs(stores.leadership(Duration.ofMillis(15_000)));
  }

  @Test
  void differentArgumentsBuildDifferentInstances() {
    BackendStores stores = open("different.db");
    assertThat(stores.eventStoreFactory(options()))
        .isNotSameAs(stores.eventStoreFactory(options(null, null, null, null, Duration.ZERO)));
    assertThat(stores.outboxStore(STRICT))
        .isNotSameAs(
            stores.outboxStore(
                new OutboxOptions(
                    OutboxOptions.DEFAULT_CLAIM_LEASE, OutboxOrderingMode.AVAILABILITY_FIRST)));
  }

  @Test
  void theSnapshotPurgerIsNewOnEveryCall() {
    BackendStores stores = open("purger.db");
    assertThat(stores.snapshotPurger(subject -> List.of()))
        .isNotSameAs(stores.snapshotPurger(subject -> List.of()));
  }

  @Test
  void nullArgumentsAreRefused() {
    BackendStores stores = open("nulls.db");
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
  void aLeaseTtlThatIsNotPositiveIsRefused() {
    BackendStores stores = open("ttl.db");
    assertThatThrownBy(() -> stores.leadership(Duration.ZERO))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("leaseTtl must be a positive duration");
    assertThatThrownBy(() -> stores.leadership(Duration.ofSeconds(-1)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("leaseTtl must be a positive duration");
  }

  // ---------------------------------------------------------------- the event store factory

  @Test
  void aForeignOutboxIsRefusedWithTheR1MessageWithoutTouchingTheDatabase() throws Exception {
    Path file = dir.resolve("foreign-outbox.db");
    CountingDataSource counting = new CountingDataSource(TestDatabases.dataSource(file));
    BackendStores stores = open(counting, null);
    assertThatThrownBy(
            () ->
                stores.eventStoreFactory(
                    options(new InMemoryOutboxStore(), MAPPER, null, null, null)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "outbox store of backend 'InMemoryOutboxStore' cannot share the 'sqlite' event"
                + " store's transaction; build it from this backend's BackendStores");
    assertThat(counting.connections()).isEqualTo(1);
    assertThat(tableCount(file)).isZero();
  }

  @Test
  void aForeignCommandInboxIsRefusedWithTheR1Message() {
    BackendStores stores = open("foreign-inbox.db");
    assertThatThrownBy(
            () ->
                stores.eventStoreFactory(
                    options(null, null, new InMemoryCommandInbox(), null, null)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "command inbox store of backend 'InMemoryCommandInbox' cannot share the 'sqlite'"
                + " event store's transaction; build it from this backend's BackendStores");
  }

  @Test
  void aForeignEventAuditStoreIsRefusedWithTheR1Message() {
    BackendStores stores = open("foreign-audit.db");
    assertThatThrownBy(
            () ->
                stores.eventStoreFactory(
                    options(null, null, null, new InMemoryEventAuditStore(), null)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "event audit store of backend 'InMemoryEventAuditStore' cannot share the 'sqlite'"
                + " event store's transaction; build it from this backend's BackendStores");
  }

  /** A Postgres store is named by its backend, not its class. */
  @Test
  void theRefusalNamesTheBackendThatOwnsTheStore() {
    BackendStores stores = open("postgres-owned.db");
    var postgresAudit = new PostgresEventAuditStore(TestDatabases.dataSource(dir.resolve("x.db")));
    assertThatThrownBy(
            () -> stores.eventStoreFactory(options(null, null, null, postgresAudit, null)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "event audit store of backend 'postgres' cannot share the 'sqlite' event store's"
                + " transaction; build it from this backend's BackendStores");
  }

  @Test
  void anAnonymousForeignStoreIsNamedByItsClassName() {
    BackendStores stores = open("anonymous.db");
    var anonymous =
        new EventAuditStore() {
          @Override
          public void save(EventAuditEntry entry) {}

          @Override
          public void saveAll(List<EventAuditEntry> entries) {}
        };
    assertThatThrownBy(() -> stores.eventStoreFactory(options(null, null, null, anonymous, null)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "event audit store of backend '" + anonymous.getClass().getName() + "' cannot share");
  }

  /** SQLite has no statement timeout: every value is accepted and ignored. */
  @Test
  void theStatementTimeoutIsIgnored() {
    BackendStores stores = open("timeout.db");
    for (Duration timeout :
        new Duration[] {
          Duration.ZERO, Duration.ofNanos(500_000), Duration.ofSeconds(-1), Duration.ofDays(400)
        }) {
      assertThatCode(() -> stores.eventStoreFactory(options(null, null, null, null, timeout)))
          .doesNotThrowAnyException();
    }
  }

  @Test
  void userDeclaredSqliteStoresAreAcceptedAndWrittenInTheAppend() throws Exception {
    Path file = dir.resolve("declared.db");
    BackendStores stores = open(TestDatabases.dataSource(file), null);
    SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(file));
    opened.add(database);
    var outbox = new SqliteOutboxStore(database, Duration.ofMinutes(1));
    EventStoreFactory factory =
        stores.eventStoreFactory(
            options(
                outbox,
                MAPPER,
                new SqliteCommandInbox(database),
                new SqliteEventAuditStore(database),
                null));

    StreamId stream = stream("declared");
    factory.create().append(stream, List.of(envelope(stream)), Version.initial());

    assertThat(outbox.loadPending(100))
        .anySatisfy(entry -> assertThat(entry.streamId()).isEqualTo(stream));
    assertThat(count(file, "SELECT count(*) FROM event_audit_log")).isEqualTo(1);
  }

  @Test
  void storesFromTheBundleAreAcceptedToo() {
    BackendStores stores = open("bundle-stores.db");
    assertThatCode(
            () ->
                stores
                    .eventStoreFactory(
                        options(
                            stores.outboxStore(STRICT),
                            MAPPER,
                            stores.commandInbox(),
                            stores.eventAuditStore(),
                            null))
                    .create())
        .doesNotThrowAnyException();
  }

  /** A SQLite store over another file is the factory's to refuse, when it creates the store. */
  @Test
  void aSqliteStoreOverAnotherFileIsRefusedByCreate() {
    BackendStores stores = open("main.db");
    SqliteDatabase other = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("other.db")));
    opened.add(other);
    EventStoreFactory factory =
        stores.eventStoreFactory(options(null, null, null, new SqliteEventAuditStore(other), null));
    assertThatThrownBy(factory::create)
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("eventAuditStore is built over");
  }

  @Test
  void anOutboxStoreWithoutAMapperConfiguresNoOutbox() {
    BackendStores stores = open("no-mapper.db");
    var outbox = (SqliteOutboxStore) stores.outboxStore(STRICT);
    EventStoreFactory factory = stores.eventStoreFactory(options(outbox, null, null, null, null));

    StreamId stream = stream("no-mapper");
    factory.create().append(stream, List.of(envelope(stream)), Version.initial());

    assertThat(outbox.loadPending(100)).isEmpty();
  }

  @Test
  void aMapperWithoutAnOutboxStoreConfiguresNoOutbox() {
    BackendStores stores = open("no-store.db");
    EventStoreFactory factory = stores.eventStoreFactory(options(null, MAPPER, null, null, null));

    StreamId stream = stream("no-store");
    assertThatCode(
            () -> factory.create().append(stream, List.of(envelope(stream)), Version.initial()))
        .doesNotThrowAnyException();
  }

  @Test
  void everyCreateOfTheMemoizedFactoryBuildsANewEventStore() {
    EventStoreFactory factory = open("creates.db").eventStoreFactory(options());
    EventStore first = factory.create();
    EventStore second = factory.create();
    assertThat(first).isExactlyInstanceOf(SqliteEventStore.class);
    assertThat(second).isExactlyInstanceOf(SqliteEventStore.class).isNotSameAs(first);
  }

  @Test
  void theEventStoreTakesTheContextsCryptoEngine() throws Exception {
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
    Path blindFile = dir.resolve("crypto-blind.db");
    EventStoreFactory blind =
        open(TestDatabases.dataSource(blindFile), null).eventStoreFactory(secretOptions);
    assertThatThrownBy(blind::create).isInstanceOf(IllegalStateException.class);
    assertThat(tableCount(blindFile)).isZero();

    EventStoreFactory withEngine =
        open(TestDatabases.dataSource(dir.resolve("crypto.db")), engine())
            .eventStoreFactory(secretOptions);
    assertThat(withEngine.create()).isExactlyInstanceOf(SqliteEventStore.class);
  }

  @Test
  void theStoresStampWithTheContextsClock() throws Exception {
    Instant fixed = Instant.parse("2026-01-02T03:04:05Z");
    Path file = dir.resolve("clock.db");
    BackendStores stores =
        openWith(
            new BackendContext(
                TestDatabases.dataSource(file),
                null,
                Clock.fixed(fixed, java.time.ZoneOffset.UTC),
                StreamRuneMetrics.NOOP,
                StreamRuneTracing.NOOP,
                true,
                true,
                true));
    StreamId stream = stream("clock");
    stores
        .eventStoreFactory(options())
        .create()
        .append(stream, List.of(envelope(stream)), Version.initial());
    assertThat(count(file, "SELECT created_at FROM event_stream"))
        .isEqualTo(fixed.getEpochSecond() * 1_000_000L);

    stores
        .defaultReadModelStore()
        .checkpoints()
        .saveOffset(org.streamrune.core.types.ProjectionName.of("clocked"), GlobalOffset.of(1));
    assertThat(
            count(
                file,
                "SELECT updated_at FROM readmodel_checkpoint WHERE projection_name = 'clocked'"))
        .isEqualTo(fixed.getEpochSecond() * 1_000_000L);
  }

  // ---------------------------------------------------------------- close

  /**
   * After close() every store accessor refuses, built before or not, and so does the create() of a
   * factory obtained before: a store handed out after the guard is released would write without it.
   * close() is idempotent; the name still answers.
   */
  @Test
  void afterCloseEveryStoreAccessorRefuses() {
    BackendStores stores = new SqliteBackend().open(context(dataSource("closed.db"), null));
    // Built before close: the memoized instances are refused too, not handed back.
    EventStoreFactory factory = stores.eventStoreFactory(options());
    stores.offsetStore();
    stores.defaultReadModelStore();
    stores.sagaStore();
    stores.outboxStore(STRICT);
    stores.close();
    assertThatCode(stores::close).doesNotThrowAnyException();
    assertThat(stores.backendName()).isEqualTo("sqlite");

    assertThatThrownBy(factory::create)
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("is closed");
    List<org.assertj.core.api.ThrowableAssert.ThrowingCallable> accessors =
        List.of(
            () -> stores.eventStoreFactory(options()),
            () -> stores.eventStoreFactory(options(null, null, null, null, Duration.ZERO)),
            stores::offsetStore,
            stores::defaultReadModelStore,
            stores::sagaStore,
            stores::sagaDeadLetterStore,
            () -> stores.outboxStore(STRICT),
            () ->
                stores.outboxStore(
                    new OutboxOptions(
                        Duration.ofMinutes(1), OutboxOrderingMode.AVAILABILITY_FIRST)),
            stores::commandInbox,
            stores::deadLetterQueue,
            stores::projectionDeadLetterStore,
            stores::auditStore,
            stores::eventAuditStore,
            stores::commandAuditQuery,
            stores::eventAuditQuery,
            stores::complianceReportQuery,
            () -> stores.snapshotPurger(subject -> List.of()));
    for (var accessor : accessors) {
      assertThatThrownBy(accessor)
          .isExactlyInstanceOf(IllegalStateException.class)
          .hasMessageContaining("are closed")
          .hasMessageContaining("without the instance guard");
    }
  }

  /**
   * The leadership is NOOP before and after close(): it holds nothing, so closing it is a no-op.
   */
  @Test
  void theLeadershipIsNoopBeforeAndAfterClose() {
    BackendStores stores = new SqliteBackend().open(context(dataSource("leader.db"), null));
    SubscriptionLeadership before = stores.leadership(Duration.ofSeconds(15));
    stores.close();
    assertThat(before).isSameAs(SubscriptionLeadership.NOOP);
    assertThat(stores.leadership(Duration.ofSeconds(20))).isSameAs(SubscriptionLeadership.NOOP);
  }

  // ---------------------------------------------------------------- verify

  @Test
  void verifyAcceptsTheDatabaseUnderTestWithoutTakingTheGuard() throws Exception {
    Path file = dir.resolve("verify.db");
    assertThatCode(() -> new SqliteBackend().verify(TestDatabases.dataSource(file)))
        .doesNotThrowAnyException();
    assertThat(ForkedJvm.probe(file)).startsWith(GuardChild.ACQUIRED);
    assertThat(tableCount(file)).isZero();
  }

  @Test
  void verifyRefusesAnotherProductNamingBoth() {
    assertThatThrownBy(() -> new SqliteBackend().verify(server("PostgreSQL", "17.5")))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("sqlite")
        .hasMessageContaining("PostgreSQL 17.5");
    assertThatThrownBy(() -> new SqliteBackend().verify(server("H2", "2.3.232 (2024-08-11)")))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("sqlite")
        .hasMessageContaining("H2");
  }

  @Test
  void verifyRunsTheSetupChecksOfOpen() {
    assertThatThrownBy(() -> new SqliteBackend().verify(TestDatabases.url("jdbc:sqlite::memory:")))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("in-memory");
    assertThatThrownBy(
            () ->
                new SqliteBackend()
                    .verify(
                        TestDatabases.dataSource(
                            dir.resolve("verify-sync.db"), "synchronous=NORMAL")))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("synchronous");
  }

  @Test
  void verifyReportsAnUnreachableDatabaseAsAnEventStoreException() {
    DataSource unreachable =
        dataSourceProxy(
            (proxy, method, args) -> {
              if (method.getName().equals("getConnection")) {
                throw new SQLException("unable to open database file");
              }
              throw new UnsupportedOperationException(method.getName());
            });
    assertThatThrownBy(() -> new SqliteBackend().verify(unreachable))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("unable to open database file")
        .hasCauseExactlyInstanceOf(SQLException.class);
  }

  @Test
  void verifyNeedsADataSource() {
    assertThatThrownBy(() -> new SqliteBackend().verify(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("dataSource is required");
  }

  // ---------------------------------------------------------------- helpers

  private DataSource dataSource(String name) {
    return TestDatabases.dataSource(dir.resolve(name));
  }

  private BackendStores open(String name) {
    return open(dataSource(name), null);
  }

  private BackendStores open(DataSource dataSource, CryptoEngine cryptoEngine) {
    return openWith(context(dataSource, cryptoEngine));
  }

  private BackendStores openWith(BackendContext context) {
    BackendStores stores = new SqliteBackend().open(context);
    opened.add(stores);
    return stores;
  }

  /** A bundle whose event-store series is applied, so every store can be built and used. */
  private BackendStores migrated(String name) {
    BackendStores stores = open(name);
    stores.eventStoreFactory(options()).create();
    return stores;
  }

  private static CryptoEngine engine() {
    return new InMemoryCryptoEngine();
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
      CommandInbox inbox,
      EventAuditStore audit,
      Duration statementTimeout) {
    return new EventStoreOptions(
        REGISTRY, List.of(), List.of(), outbox, mapper, inbox, audit, statementTimeout);
  }

  private static StreamId stream(String id) {
    return StreamId.of(AggregateType.of("order"), AggregateId.of(id));
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

  private static Object field(Object target, String name) throws ReflectiveOperationException {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  private static long tableCount(Path file) throws SQLException {
    return count(file, "SELECT count(*) FROM sqlite_schema");
  }

  private static boolean tableExists(Path file, String table) throws SQLException {
    return count(
            file,
            "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = '" + table + "'")
        > 0;
  }

  /** Reads one number from {@code file} on a connection of its own, outside every bundle. */
  private static long count(Path file, String sql) throws SQLException {
    try (Connection connection = java.sql.DriverManager.getConnection("jdbc:sqlite:file:" + file);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  /** A data source whose connections report {@code product} {@code version} and nothing else. */
  private static DataSource server(String product, String version) {
    DatabaseMetaData meta =
        (DatabaseMetaData)
            Proxy.newProxyInstance(
                SqliteBackendTest.class.getClassLoader(),
                new Class<?>[] {DatabaseMetaData.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "getDatabaseProductName" -> product;
                      case "getDatabaseProductVersion" -> version;
                      default -> throw new UnsupportedOperationException(method.getName());
                    });
    Connection connection =
        (Connection)
            Proxy.newProxyInstance(
                SqliteBackendTest.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "getMetaData" -> meta;
                      case "close" -> null;
                      default -> throw new UnsupportedOperationException(method.getName());
                    });
    return dataSourceProxy(
        (proxy, method, args) -> {
          if (method.getName().equals("getConnection")) {
            return connection;
          }
          throw new UnsupportedOperationException(method.getName());
        });
  }

  private static DataSource dataSourceProxy(java.lang.reflect.InvocationHandler handler) {
    return (DataSource)
        Proxy.newProxyInstance(
            SqliteBackendTest.class.getClassLoader(), new Class<?>[] {DataSource.class}, handler);
  }

  /** Counts every connection checkout; delegates everything to the wrapped data source. */
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
