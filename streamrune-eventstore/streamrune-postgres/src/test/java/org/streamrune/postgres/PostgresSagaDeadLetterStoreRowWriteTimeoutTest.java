package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The {@code saga_state} statements of {@link PostgresSagaDeadLetterStore} wait at most the
 * saga-row write bound, and its transactions that touch a saga row are ended by the same bound when
 * their client goes silent inside them: a dead-lettering delivery behind another transaction
 * holding the saga row fails with SQLState {@code 57014} instead of waiting for as long as the
 * holder lives, and a silent shield transaction releases the row within the bound.
 */
@Testcontainers
class PostgresSagaDeadLetterStoreRowWriteTimeoutTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("saga_shield_bound");

  record ShieldState(String step) implements SagaState {
    @Override
    public SagaStatus status() {
      return SagaStatus.RUNNING;
    }
  }

  private static final SagaType TYPE = SagaType.fromClass(ShieldState.class);
  private static final SagaId ID = SagaId.of("shielded-saga-1");
  private static final long BOUND_MILLIS = 500;
  private static final long SLACK_MILLIS = 1_000;

  static PGSimpleDataSource dataSource;

  private final List<Connection> holders = new ArrayList<>();
  private PostgresSagaDeadLetterStore deadLetters;
  private PostgresSagaStore sagas;

  @BeforeAll
  static void schema() {
    dataSource = dataSource("shield-test");
    new PostgresEventStoreFactory(dataSource, NO_EVENTS).create();
  }

  @BeforeEach
  void row() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("TRUNCATE saga_state, saga_dead_letters");
    }
    deadLetters = new PostgresSagaDeadLetterStore(dataSource, Duration.ofMillis(BOUND_MILLIS));
    sagas =
        new PostgresSagaStore(
            dataSource,
            new ObjectMapper().registerModule(new JavaTimeModule()),
            Duration.ofMillis(BOUND_MILLIS));
    sagas.create(ID, TYPE, new ShieldState("start"), SagaStatus.RUNNING, false);
  }

  @AfterEach
  void releaseHolders() throws SQLException {
    for (Connection holder : holders) {
      holder.rollback();
      holder.close();
    }
    holders.clear();
  }

  @Test
  void aShieldedPublishBehindAHeldSagaRowFailsAtItsBound_andSucceedsOnceTheRowIsFree()
      throws Exception {
    Connection holder = holdTheRow();

    long started = System.nanoTime();
    Throwable thrown = failureWithinCap(() -> deadLetters.publishShielded(entry(1)));
    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

    assertThat(thrown).isInstanceOf(EventStoreException.class);
    assertThat(sqlStateOf(thrown)).isEqualTo("57014");
    assertThat(elapsed).isLessThan(BOUND_MILLIS + SLACK_MILLIS);
    assertThat(deadLetters.findBySaga(ID)).as("the entry is rolled back with it").isEmpty();

    holder.rollback();
    deadLetters.publishShielded(entry(1));
    assertThat(deadLetters.findBySaga(ID)).hasSize(1);
    assertThat(shield()).isTrue();
  }

  @Test
  void aShieldClearBehindAHeldSagaRowFailsAtItsBound_andSucceedsOnceTheRowIsFree()
      throws Exception {
    sagas.setDeadLetterPending(ID, TYPE, true);
    Connection holder = holdTheRow();

    long started = System.nanoTime();
    Throwable thrown = failureWithinCap(() -> deadLetters.clearShieldIfDrained(ID, TYPE));
    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

    assertThat(thrown).isInstanceOf(EventStoreException.class);
    assertThat(sqlStateOf(thrown)).isEqualTo("57014");
    assertThat(elapsed).isLessThan(BOUND_MILLIS + SLACK_MILLIS);
    assertThat(shield()).isTrue();

    holder.rollback();
    assertThat(deadLetters.clearShieldIfDrained(ID, TYPE)).isTrue();
    assertThat(shield()).isFalse();
  }

  /**
   * A shield clear whose client goes silent after it locked the saga row — before its conditional
   * update and its commit — is ended by its transaction's idle bound: the row is free again within
   * the bound, a write of the saga row goes through, and the silent call fails once it goes on.
   */
  @Test
  void aShieldClearSilentInsideItsTransactionIsEndedAtTheBound() throws Exception {
    sagas.setDeadLetterPending(ID, TYPE, true);
    PausingDataSource pausing = new PausingDataSource(dataSource("silent-clear"));
    PostgresSagaDeadLetterStore silent =
        new PostgresSagaDeadLetterStore(pausing, Duration.ofMillis(BOUND_MILLIS));
    PausingDataSource.Pause pause = pausing.pauseBefore("UPDATE saga_state");
    CompletableFuture<Boolean> clearing =
        CompletableFuture.supplyAsync(() -> silent.clearShieldIfDrained(ID, TYPE));
    pause.awaitPaused();
    long idleSince = System.nanoTime();

    awaitNoBackendNamed("silent-clear", BOUND_MILLIS + SLACK_MILLIS, idleSince);
    sagas.setDeadLetterPending(ID, TYPE, false);
    assertThat(shield()).as("the row was free for another write").isFalse();

    pause.release();
    Throwable thrown = catchFailure(clearing);
    assertThat(thrown).isInstanceOf(EventStoreException.class);
  }

  // ---------------------------------------------------------------------------------------------

  private static SagaDeadLetterEntry entry(long offset) {
    return new SagaDeadLetterEntry(
        ID,
        TYPE,
        GlobalOffset.of(offset),
        EventType.of("OrderCreated"),
        "java.lang.RuntimeException",
        "boom",
        Instant.now().truncatedTo(ChronoUnit.MICROS));
  }

  /** Another transaction holding the saga row's lock. */
  private Connection holdTheRow() throws SQLException {
    Connection holder = dataSource.getConnection();
    holder.setAutoCommit(false);
    holders.add(holder);
    try (PreparedStatement ps =
        holder.prepareStatement("SELECT 1 FROM saga_state WHERE saga_id = ? FOR UPDATE")) {
      ps.setString(1, ID.value());
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
      }
    }
    return holder;
  }

  private boolean shield() {
    return sagas.load(ID, TYPE, ShieldState.class).orElseThrow().deadLetterPending();
  }

  private static Throwable failureWithinCap(Runnable call) {
    return catchFailure(CompletableFuture.runAsync(call));
  }

  private static <T> Throwable catchFailure(CompletableFuture<T> running) {
    try {
      running.get(10, TimeUnit.SECONDS);
      return null;
    } catch (ExecutionException e) {
      return e.getCause();
    } catch (TimeoutException | InterruptedException e) {
      throw new AssertionError("the call was still waiting after ten seconds", e);
    }
  }

  private static String sqlStateOf(Throwable thrown) {
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      if (t instanceof SQLException sql) {
        return sql.getSQLState();
      }
    }
    return null;
  }

  private static void awaitNoBackendNamed(String applicationName, long boundMillis, long since)
      throws Exception {
    Supplier<Long> elapsed = () -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - since);
    while (true) {
      try (Connection conn = dataSource.getConnection();
          PreparedStatement ps =
              conn.prepareStatement(
                  "SELECT count(*) FROM pg_stat_activity WHERE application_name = ?")) {
        ps.setString(1, applicationName);
        try (ResultSet rs = ps.executeQuery()) {
          rs.next();
          if (rs.getInt(1) == 0) {
            return;
          }
        }
      }
      assertThat(elapsed.get())
          .as("the backend of " + applicationName + " is ended within " + boundMillis + "ms")
          .isLessThan(boundMillis);
      TimeUnit.MILLISECONDS.sleep(20);
    }
  }

  private static PGSimpleDataSource dataSource(String applicationName) {
    PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setUrl(PG.getJdbcUrl());
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());
    ds.setApplicationName(applicationName);
    return ds;
  }

  private static final EventTypeRegistry NO_EVENTS =
      new EventTypeRegistry() {
        @Override
        public Class<?> resolveEventType(EventType eventType) {
          throw new IllegalArgumentException("no events here: " + eventType);
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          throw new IllegalArgumentException("no snapshots here: " + stateType);
        }

        @Override
        public Collection<Class<?>> registeredTypes() {
          return List.of();
        }
      };
}
