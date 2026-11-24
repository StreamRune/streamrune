package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus.CommandResult;
import org.streamrune.core.CommandBus.ShortCircuitReason;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.backend.EventStoreOptions;
import org.streamrune.core.backend.OutboxOptions;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.UserId;
import org.streamrune.crypto.CryptoBackend;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.crypto.sqlite.SqliteCryptoBackend;
import org.streamrune.integration.tck.SingleInstanceMultiReadModelStoreScenario;
import org.streamrune.integration.tck.SqliteTestDatabases;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.runtime.gdpr.ForgetResult;
import org.streamrune.runtime.gdpr.ForgetSubjectService;
import org.streamrune.sqlite.SqliteBackend;

/**
 * A plain-Java application on the SQLite backend, with no DI container: the whole programming model
 * runs on one SQLite file, and the application closes and opens again on the same file.
 *
 * <p>{@link SqliteApplication} wires what an integration would: a HikariCP pool on the file with
 * the connection settings the backend requires, a {@code SqliteCryptoEngine} from {@link
 * SqliteCryptoBackend}, the store bundle from {@code new SqliteBackend().open(context)} and, built
 * from the bundle alone, an event store with an outbox and a command inbox, a command bus with two
 * deciders, a {@code TRANSACTIONAL_LOCAL} projection over the bundle's default read-model store,
 * and a saga whose step dispatches a command. Closing stops the runners and the bus, then closes
 * the bundle, then the pool: a store still in use after the bundle closed would write without the
 * instance guard.
 *
 * <p>The test asserts the state each part leaves in the file: the events (one, under an idempotency
 * key sent twice), the read model and its checkpoint, the saga row, the outbox row through the
 * {@link OutboxStore} API, and, after a GDPR forget run from application code, the shredded
 * {@code @Encrypted} field of both the read model and the event, with one erasure row for the
 * subject in the audit log. While either application runs, another process cannot open the file.
 * The saga subscription's checkpoint is on the file between the two runs. Opened again with a new
 * pool and a new bundle, the projection resumes from its checkpoint without applying any earlier
 * event again, and the saga and outbox state is what the first run left.
 */
@Timeout(180)
class SqliteEndToEndIT {

  private static final AggregateType CUSTOMER = AggregateType.of("customer");
  private static final AggregateType WELCOME = AggregateType.of("welcome");
  private static final ProjectionName SUMMARY = ProjectionName.of("customer_summary");
  private static final SagaType WELCOME_SAGA = SagaType.fromClass(WelcomeSagaState.class);
  private static final Duration PATIENCE = Duration.ofSeconds(30);
  private static final String WELCOME_SUBSCRIPTION = "welcome-saga";

  private static final EventTypeRegistry TYPES =
      SimpleEventTypeRegistry.builder()
          .registerEvent(CustomerRegistered.class)
          .registerEvent(WelcomeSent.class)
          .build();

  // ---- the domain ----------------------------------------------------------------------------

  record RegisterCustomer(String customerId, String email) implements Command {}

  /** The event that carries personal data: the email is encrypted under the customer's key. */
  record CustomerRegistered(String customerId, @Encrypted(subjectId = "customerId") String email)
      implements DomainEvent {}

  record CustomerState(boolean registered) implements AggregateState {}

  static final class CustomerDecider
      implements Decider<RegisterCustomer, CustomerState, CustomerRegistered> {
    @Override
    public CustomerState initialState() {
      return new CustomerState(false);
    }

    @Override
    public List<CustomerRegistered> decide(RegisterCustomer command, CustomerState state) {
      if (state.registered()) {
        throw new IllegalStateException("customer " + command.customerId() + " is registered");
      }
      return List.of(new CustomerRegistered(command.customerId(), command.email()));
    }

    @Override
    public CustomerState evolve(CustomerState state, CustomerRegistered event) {
      return new CustomerState(true);
    }
  }

  record SendWelcome(String customerId) implements Command {}

  record WelcomeSent(String customerId) implements DomainEvent {}

  record WelcomeState() implements AggregateState {}

  static final class WelcomeDecider implements Decider<SendWelcome, WelcomeState, WelcomeSent> {
    @Override
    public WelcomeState initialState() {
      return new WelcomeState();
    }

    @Override
    public List<WelcomeSent> decide(SendWelcome command, WelcomeState state) {
      return List.of(new WelcomeSent(command.customerId()));
    }

    @Override
    public WelcomeState evolve(WelcomeState state, WelcomeSent event) {
      return state;
    }
  }

  // ---- the saga: a registration starts it, its step sends the welcome --------------------------

  record WelcomeSagaState(SagaStatus status, String customerId) implements SagaState {
    @JsonCreator
    WelcomeSagaState(
        @JsonProperty("status") SagaStatus status, @JsonProperty("customerId") String customerId) {
      this.status = status;
      this.customerId = customerId;
    }
  }

  static final class WelcomeSaga implements SagaOrchestrator<WelcomeSagaState> {
    @Override
    public Class<WelcomeSagaState> stateType() {
      return WelcomeSagaState.class;
    }

    @Override
    public WelcomeSagaState initialState(SagaId sagaId) {
      return new WelcomeSagaState(SagaStatus.STARTED, null);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof CustomerRegistered;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return sagaIdOf(((CustomerRegistered) event.event()).customerId());
    }

    @Override
    public WelcomeSagaState evolve(WelcomeSagaState state, EventEnvelope event) {
      return switch (event.event()) {
        case CustomerRegistered registered ->
            new WelcomeSagaState(SagaStatus.RUNNING, registered.customerId());
        case WelcomeSent sent -> new WelcomeSagaState(SagaStatus.COMPLETED, sent.customerId());
        default -> state;
      };
    }

    @Override
    public List<SagaCommand> handle(WelcomeSagaState state, EventEnvelope event) {
      if (event.event() instanceof CustomerRegistered registered) {
        return List.of(
            SagaCommand.of(
                new SendWelcome(registered.customerId()), AggregateId.of(registered.customerId())));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        WelcomeSagaState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  static SagaId sagaIdOf(String customerId) {
    return SagaId.of("welcome-" + customerId);
  }

  // ---- the read model -------------------------------------------------------------------------

  /**
   * One row per customer. {@code applications} counts the committed applications of the
   * registration to this row: a value above 1 means the event was applied again.
   */
  record CustomerSummary(
      String customerId, @Encrypted(subjectId = "customerId") String email, int applications) {}

  /**
   * Writes {@link CustomerSummary} rows through the batch's repository, so the rows and the
   * checkpoint commit in one transaction ({@code TRANSACTIONAL_LOCAL}). It also counts, in memory,
   * every offset it is handed: the second application's instance must be handed none of the offsets
   * the first one committed.
   */
  static final class CustomerSummaryProjection implements Projection {
    final Map<Long, AtomicInteger> offsetsHanded = new ConcurrentHashMap<>();

    @Override
    public void process(List<EventEnvelope> batch) {
      throw new IllegalStateException("runs through the repository overload");
    }

    @Override
    public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
      for (EventEnvelope envelope : batch) {
        offsetsHanded
            .computeIfAbsent(envelope.globalOffset().value(), offset -> new AtomicInteger())
            .incrementAndGet();
        if (envelope.event() instanceof CustomerRegistered registered) {
          int before =
              repository
                  .findById(SUMMARY, registered.customerId(), CustomerSummary.class)
                  .map(CustomerSummary::applications)
                  .orElse(0);
          repository.save(
              SUMMARY,
              registered.customerId(),
              new CustomerSummary(registered.customerId(), registered.email(), before + 1));
        }
      }
    }

    @Override
    public boolean writesThroughRepository() {
      return true;
    }
  }

  /** The outbox entry of a registration: the customer id only, no personal data. */
  static List<OutboxEntry> toOutbox(EventEnvelope envelope) {
    if (!(envelope.event() instanceof CustomerRegistered registered)) {
      return List.of();
    }
    return List.of(
        OutboxEntry.pending(
            OutboxEntryId.of("registered-" + registered.customerId()),
            "{\"customerId\":\"" + registered.customerId() + "\"}",
            "CustomerRegistered",
            envelope.streamId()));
  }

  // ---- the application ------------------------------------------------------------------------

  /**
   * The application, wired by hand. Everything is built from the bundle the backend opens; nothing
   * names a SQLite store class.
   */
  static final class SqliteApplication implements AutoCloseable {
    final HikariDataSource pool;
    final CryptoEngine cryptoEngine;
    final BackendStores stores;
    final ReadModelStore readModels;
    final OutboxStore outbox;
    final EventStore eventStore;
    final VirtualThreadCommandBus commandBus;
    final CustomerSummaryProjection projection = new CustomerSummaryProjection();
    final MultiProjectionRunner projectionRunner;
    final PollingEventSubscription sagaSubscription;

    SqliteApplication(Path file) {
      // The pool: SQLite's required connection settings as URL parameters (WAL, synchronous=FULL,
      // foreign keys, busy_timeout, transaction_mode=IMMEDIATE), autocommit connections.
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(SqliteTestDatabases.url(file));
      config.setPoolName("e2e-sqlite");
      pool = new HikariDataSource(config);

      // The key store on the same file. The call checks the file and its guard, and holds neither.
      cryptoEngine =
          new SqliteCryptoBackend()
              .keyStoreEngine(
                  pool, new CryptoBackend.KeyStoreEngineSettings(null, StreamRuneMetrics.NOOP));

      // The bundle: one handle on the file, the instance guard held until close().
      stores =
          new SqliteBackend()
              .open(
                  new BackendContext(
                      pool,
                      cryptoEngine,
                      Clock.systemUTC(),
                      StreamRuneMetrics.NOOP,
                      StreamRuneTracing.NOOP,
                      true,
                      true,
                      true));
      readModels = stores.defaultReadModelStore();
      outbox =
          stores.outboxStore(
              new OutboxOptions(Duration.ofMinutes(4), OutboxOrderingMode.STRICT_PER_AGGREGATE));
      eventStore =
          stores
              .eventStoreFactory(
                  new EventStoreOptions(
                      TYPES,
                      List.of(),
                      List.of(),
                      outbox,
                      SqliteEndToEndIT::toOutbox,
                      stores.commandInbox(),
                      null,
                      null))
              .create();
      commandBus =
          VirtualThreadCommandBus.builder()
              .eventStore(eventStore)
              .commandInbox(stores.commandInbox())
              .register(
                  CUSTOMER,
                  RegisterCustomer.class,
                  command -> AggregateId.of(command.customerId()),
                  new CustomerDecider())
              .register(
                  WELCOME,
                  SendWelcome.class,
                  command -> AggregateId.of(command.customerId()),
                  new WelcomeDecider())
              .build();

      SubscriptionConfig polling = SubscriptionConfig.pollingOnly(Duration.ofMillis(50));
      projectionRunner =
          MultiProjectionRunner.builder()
              .eventStore(eventStore)
              .subscriptionConfig(polling)
              .leadership(stores.leadership(Duration.ofSeconds(30)))
              .register(
                  SUMMARY,
                  projection,
                  ProjectionErrorStrategy.HALT,
                  ProjectionDeliveryMode.TRANSACTIONAL_LOCAL,
                  readModels.checkpointTarget())
              .build();

      SagaRunner<WelcomeSagaState> sagaRunner =
          SagaRunner.<WelcomeSagaState>builder()
              .orchestrator(new WelcomeSaga())
              .sagaStore(stores.sagaStore())
              .sagaDeadLetterStore(stores.sagaDeadLetterStore())
              .commandBus(commandBus)
              .build();
      sagaSubscription =
          new PollingEventSubscription(
              SubscriptionName.of(WELCOME_SUBSCRIPTION),
              eventStore,
              stores.offsetStore(),
              sagaRunner.asEventListener(),
              polling,
              50);

      projectionRunner.start();
      sagaSubscription.start();
    }

    /** Runners first, then the bus, then the bundle (releasing the guard), then the pool. */
    @Override
    public void close() {
      projectionRunner.close();
      sagaSubscription.close();
      commandBus.close();
      stores.close();
      pool.close();
    }

    ForgetSubjectService forgetService() {
      return ForgetSubjectService.builder()
          .cryptoEngine(cryptoEngine)
          .auditStore(stores.auditStore())
          .purgers(
              List.of(
                  stores.snapshotPurger(
                      subject -> List.of(StreamId.of(CUSTOMER, AggregateId.of(subject.value()))))))
          .build();
    }

    Optional<CustomerSummary> summary(String customerId) {
      return readModels.repository().findById(SUMMARY, customerId, CustomerSummary.class);
    }

    GlobalOffset checkpoint() {
      return readModels.checkpoints().getLastOffset(SUMMARY);
    }

    GlobalOffset sagaCheckpoint() {
      return stores.offsetStore().getLastOffset(ProjectionName.of(WELCOME_SUBSCRIPTION));
    }

    Optional<LoadedSaga<WelcomeSagaState>> saga(String customerId) {
      return stores.sagaStore().load(sagaIdOf(customerId), WELCOME_SAGA, WelcomeSagaState.class);
    }

    List<EventEnvelope> events(AggregateType type, String id) {
      return eventStore.load(StreamId.of(type, AggregateId.of(id))).events();
    }

    void awaitCaughtUp(String customerId) {
      await()
          .atMost(PATIENCE)
          .untilAsserted(
              () -> {
                assertThat(saga(customerId))
                    .as("the welcome saga of %s completes", customerId)
                    .hasValueSatisfying(
                        saga -> assertThat(saga.status()).isEqualTo(SagaStatus.COMPLETED));
                assertThat(checkpoint())
                    .as("the projection reaches the head of the event store")
                    .isEqualTo(eventStore.lastGlobalOffset());
              });
    }
  }

  // ---- the test -------------------------------------------------------------------------------

  @Test
  void aPlainJavaApplicationRunsTheWholeModelOnOneFileAndSurvivesAReopen(@TempDir Path directory)
      throws Exception {
    Path file = directory.resolve("app.sqlite");
    long adaRegistration;
    long checkpointAtClose;
    long sagaCheckpointAtClose;
    OutboxEntryId adaEntry = OutboxEntryId.of("registered-c-ada");

    try (SqliteApplication app = new SqliteApplication(file)) {
      // Guard: another process cannot open the file while the bundle holds it.
      SingleInstanceMultiReadModelStoreScenario.assertGuardHeld(file, "the application's file");

      // Decider, keyed: the second send of the same key appends nothing.
      IdempotencyKey key = IdempotencyKey.of("register-c-ada");
      CommandResult first =
          app.commandBus.execute(new RegisterCustomer("c-ada", "ada@example.com"), key);
      assertThat(first.reason()).isEqualTo(ShortCircuitReason.NONE);
      assertThat(first.globalOffsets()).hasSize(1);
      adaRegistration = first.globalOffsets().getFirst().value();
      CommandResult replay =
          app.commandBus.execute(new RegisterCustomer("c-ada", "ada@example.com"), key);
      assertThat(replay.reason()).isEqualTo(ShortCircuitReason.IDEMPOTENT_REPLAY);
      assertThat(app.events(CUSTOMER, "c-ada"))
          .singleElement()
          .satisfies(
              envelope ->
                  assertThat(envelope.event())
                      .isEqualTo(new CustomerRegistered("c-ada", "ada@example.com")));
      assertThat(
              scalar(
                  app.pool,
                  "SELECT payload FROM event_stream WHERE global_offset = ?",
                  adaRegistration))
          .as("the email is ciphertext at rest")
          .doesNotContain("ada@example.com");

      // The saga's step sends the welcome; the projection catches up with both events.
      app.awaitCaughtUp("c-ada");
      assertThat(app.events(WELCOME, "c-ada"))
          .singleElement()
          .satisfies(
              envelope ->
                  assertThat(envelope.metadata().sagaOrigin())
                      .isEqualTo(new SagaOrigin(WELCOME_SAGA, sagaIdOf("c-ada"), 0)));
      assertThat(app.summary("c-ada")).contains(new CustomerSummary("c-ada", "ada@example.com", 1));
      assertThat(app.projection.offsetsHanded.get(adaRegistration)).hasValue(1);

      // Outbox: the registration's row, claimed and marked delivered through the store's API.
      List<OutboxEntry> pending = app.outbox.loadPending(10);
      assertThat(pending)
          .singleElement()
          .satisfies(
              entry -> {
                assertThat(entry.id()).isEqualTo(adaEntry);
                assertThat(entry.payload()).isEqualTo("{\"customerId\":\"c-ada\"}");
                assertThat(entry.streamId())
                    .isEqualTo(StreamId.of(CUSTOMER, AggregateId.of("c-ada")));
              });
      assertThat(app.outbox.markDelivered(adaEntry, app.outbox.claimedBy())).isTrue();
      assertThat(app.outbox.findById(adaEntry))
          .hasValueSatisfying(
              entry -> assertThat(entry.status()).isEqualTo(OutboxStatus.DELIVERED));
      assertThat(app.outbox.loadPending(10)).isEmpty();

      // GDPR forget, from application code (never from inside a projection batch).
      ForgetResult forgotten = app.forgetService().forget(SubjectId.of("c-ada"), UserId.of("dpo"));
      assertThat(forgotten.fullyErased()).isTrue();
      assertThat(app.cryptoEngine.isKeyAvailable(SubjectId.of("c-ada"))).isFalse();
      assertThat(app.events(CUSTOMER, "c-ada"))
          .singleElement()
          .satisfies(
              envelope ->
                  assertThat(envelope.event())
                      .isEqualTo(new CustomerRegistered("c-ada", CryptoShreddingModule.REDACTED)));
      assertThat(app.summary("c-ada"))
          .contains(new CustomerSummary("c-ada", CryptoShreddingModule.REDACTED, 1));
      assertThat(
              scalar(
                  app.pool,
                  "SELECT count(*) FROM audit_log WHERE command_type = ? AND outcome = 'SUCCESS'"
                      + " AND aggregate_id = ? AND error_message IS NULL",
                  "GDPR_FORGET",
                  SubjectId.of("c-ada").redacted()))
          .as("the erasure of c-ada is on record once, under the subject's hash")
          .isEqualTo("1");

      checkpointAtClose = app.checkpoint().value();
      assertThat(checkpointAtClose).isEqualTo(app.eventStore.lastGlobalOffset().value());
      await()
          .atMost(PATIENCE)
          .untilAsserted(
              () ->
                  assertThat(app.sagaCheckpoint())
                      .as("the saga subscription reaches the head of the event store")
                      .isEqualTo(app.eventStore.lastGlobalOffset()));
      sagaCheckpointAtClose = app.sagaCheckpoint().value();
    }

    // The saga subscription's checkpoint is on the file, read before a new subscription can move
    // it: a lost checkpoint would be re-delivered from the start and absorbed by the completed
    // saga and the command inbox, so the welcome count alone cannot show it.
    try (Connection closedFile = DriverManager.getConnection(SqliteTestDatabases.url(file));
        PreparedStatement statement =
            closedFile.prepareStatement(
                "SELECT last_offset FROM subscription_checkpoint WHERE projection_name = ?")) {
      statement.setString(1, WELCOME_SUBSCRIPTION);
      try (ResultSet rows = statement.executeQuery()) {
        assertThat(rows.next()).as("the saga subscription's checkpoint row").isTrue();
        assertThat(rows.getLong(1)).isEqualTo(sagaCheckpointAtClose);
      }
    }

    // Reopen: a new pool and a new bundle on the same file.
    try (SqliteApplication app = new SqliteApplication(file)) {
      // The reopened bundle holds the guard again.
      SingleInstanceMultiReadModelStoreScenario.assertGuardHeld(file, "the reopened file");
      assertThat(app.checkpoint().value()).isEqualTo(checkpointAtClose);
      assertThat(app.sagaCheckpoint().value()).isGreaterThanOrEqualTo(sagaCheckpointAtClose);
      assertThat(app.saga("c-ada"))
          .hasValueSatisfying(
              saga -> {
                assertThat(saga.status()).isEqualTo(SagaStatus.COMPLETED);
                assertThat(saga.state().customerId()).isEqualTo("c-ada");
              });
      assertThat(app.outbox.findById(adaEntry))
          .hasValueSatisfying(
              entry -> assertThat(entry.status()).isEqualTo(OutboxStatus.DELIVERED));

      // The command inbox survived too: the old key is still a replay.
      CommandResult replay =
          app.commandBus.execute(
              new RegisterCustomer("c-ada", "ada@example.com"),
              IdempotencyKey.of("register-c-ada"));
      assertThat(replay.reason()).isEqualTo(ShortCircuitReason.IDEMPOTENT_REPLAY);

      CommandResult bob =
          app.commandBus.execute(
              new RegisterCustomer("c-bob", "bob@example.com"),
              IdempotencyKey.of("register-c-bob"));
      long bobRegistration = bob.globalOffsets().getFirst().value();
      app.awaitCaughtUp("c-bob");

      // The projection resumed from its checkpoint: it was handed no offset it had committed,
      // and the forgotten customer's row was not written again.
      assertThat(app.projection.offsetsHanded.keySet())
          .isNotEmpty()
          .allSatisfy(offset -> assertThat(offset).isGreaterThan(checkpointAtClose))
          .contains(bobRegistration);
      assertThat(app.projection.offsetsHanded.values())
          .allSatisfy(count -> assertThat(count).hasValue(1));
      assertThat(app.summary("c-ada"))
          .contains(new CustomerSummary("c-ada", CryptoShreddingModule.REDACTED, 1));
      assertThat(app.summary("c-bob")).contains(new CustomerSummary("c-bob", "bob@example.com", 1));

      // The first saga ran once: its welcome was not sent again.
      assertThat(app.events(WELCOME, "c-ada")).hasSize(1);
      assertThat(app.events(WELCOME, "c-bob")).hasSize(1);

      // The outbox holds only the new registration.
      assertThat(app.outbox.loadPending(10))
          .extracting(OutboxEntry::id)
          .containsExactly(OutboxEntryId.of("registered-c-bob"));
    }
  }

  /** The first column of the one row {@code sql} selects for {@code parameters}, as text. */
  private static String scalar(DataSource database, String sql, Object... parameters)
      throws SQLException {
    try (Connection connection = database.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      for (int i = 0; i < parameters.length; i++) {
        statement.setObject(i + 1, parameters[i]);
      }
      try (ResultSet rows = statement.executeQuery()) {
        assertThat(rows.next()).as("one row for %s", List.of(parameters)).isTrue();
        return rows.getString(1);
      }
    }
  }
}
