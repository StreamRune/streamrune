package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.ProjectionDeadLetterReplayer;
import org.streamrune.runtime.ProjectionDeadLetterReplayer.ReplayResult;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * A dead-letter replay beside a live batch, on PostgreSQL, over a read-modify-write projection
 * ({@code findById} → change one field → {@code save}). The row {@code p-1} starts at price 100,
 * stock 10. Offset 1 ({@code PriceUpdated(150)}) is dead-lettered and the checkpoint is past it;
 * offset 2 ({@code StockAdjusted(-3)}) is the live batch.
 *
 * <table>
 * <caption>Interleavings and crash points pinned, and what each test proves</caption>
 * <tr><th>test</th><th>point</th><th>durable state there</th><th>outcome</th></tr>
 * <tr><td>replayBesideAnOpenLiveBatch…</td><td>the live batch is open after its read, or after its
 * write, when the replay starts</td><td>the live transaction holds the checkpoint row</td><td>the
 * replay waits for the checkpoint row and has not read the row yet; price 150 and stock 7 both
 * survive, the entry is discarded, the checkpoint stays at 2</td></tr>
 * <tr><td>crashBeforeTheReplayCommit…</td><td>the replay's connection dies after its write, before
 * its commit</td><td>row unchanged, entry kept, checkpoint unchanged</td><td>the next replay
 * applies the range once and discards the entry</td></tr>
 * <tr><td>crashAfterTheReplayCommit…</td><td>the discard fails after the replay committed</td><td>
 * range applied, entry kept</td><td>the next replay applies the range again (an idempotent
 * projection converges on the same row) and discards the entry</td></tr>
 * </table>
 *
 * <p>A crash after the discard leaves nothing to converge: the range is applied and its entry is
 * gone.
 */
@Testcontainers
class ProjectionDeadLetterReplayConcurrencyIT {

  private static final String APP = "replay-it";
  private static final String ROW = "p-1";

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_replay_it");

  static PGSimpleDataSource ds;
  static PGSimpleDataSource observerDs;

  @BeforeAll
  static void init() {
    ds = CrashItSupport.dataSource(PG, APP);
    observerDs = CrashItSupport.dataSource(PG, "replay-it-observer");
    org.flywaydb.core.Flyway.configure()
        .dataSource(ds)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
  }

  @BeforeEach
  void clean() throws Exception {
    try (var c = observerDs.getConnection();
        var st = c.createStatement()) {
      st.execute("DELETE FROM projection_offset");
      st.execute("DELETE FROM projection_dead_letters");
      st.execute("DROP TABLE IF EXISTS product_view");
    }
  }

  record PriceUpdated(int price) implements DomainEvent {}

  record StockAdjusted(int delta) implements DomainEvent {}

  record ProductView(
      @JsonProperty("id") String id,
      @JsonProperty("price") int price,
      @JsonProperty("stock") int stock) {
    @JsonCreator
    ProductView {}
  }

  /** Where the live batch is held open while the replay starts. */
  enum HoldPoint {
    AFTER_READ,
    AFTER_WRITE
  }

  /**
   * Read-modify-write over one row: each event reads the row, changes one field and saves it. A
   * {@code PriceUpdated} sets the price (idempotent), a {@code StockAdjusted} adds its delta.
   */
  static final class ProductProjection extends BaseProjection {
    final AtomicBoolean priceUpdateEntered = new AtomicBoolean();
    volatile Runnable afterStockRead = () -> {};
    volatile Runnable afterStockWrite = () -> {};
    volatile Runnable afterPriceWrite = () -> {};

    ProductProjection(ProjectionRepository repository) {
      super(repository, "product");
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      for (var envelope : batch) {
        if (envelope.event() instanceof PriceUpdated priceUpdated) {
          priceUpdateEntered.set(true);
          var row = findById(ROW, ProductView.class).orElseThrow();
          save(ROW, new ProductView(ROW, priceUpdated.price(), row.stock()));
          afterPriceWrite.run();
        } else if (envelope.event() instanceof StockAdjusted stockAdjusted) {
          var row = findById(ROW, ProductView.class).orElseThrow();
          afterStockRead.run();
          save(ROW, new ProductView(ROW, row.price(), row.stock() + stockAdjusted.delta()));
          afterStockWrite.run();
        }
      }
    }
  }

  /** The fixture every test starts from: the seeded row, the two events, the dead-letter entry. */
  private static final class Fixture {
    final ProjectionName name = ProjectionName.of("product");
    final JdbcProjectionRepository repo = new JdbcProjectionRepository(ds);
    final ProductProjection projection = new ProductProjection(repo);
    final PostgresOffsetStore offsets = new PostgresOffsetStore(ds);
    final PostgresProjectionDeadLetterStore deadLetters = new PostgresProjectionDeadLetterStore(ds);
    final InMemoryEventStore events = new InMemoryEventStore();

    Fixture() {
      events.append(
          TestStreams.stream(ROW),
          List.of(
              EventStoreFixture.event(
                  TestStreams.stream(ROW),
                  new Version(1),
                  new EventType("PriceUpdated"),
                  new PriceUpdated(150)),
              EventStoreFixture.event(
                  TestStreams.stream(ROW),
                  new Version(2),
                  new EventType("StockAdjusted"),
                  new StockAdjusted(-3))),
          new Version(0));
      repo.save(name, ROW, new ProductView(ROW, 100, 10));
      // The runner dead-lettered offset 1 and advanced the checkpoint past it.
      deadLetters.save(
          new ProjectionDeadLetterEntry(
              name,
              GlobalOffset.of(1),
              GlobalOffset.of(1),
              1,
              "java.lang.IllegalStateException",
              "boom",
              1,
              Instant.now()));
      repo.executeAtomically(name, List.of(), GlobalOffset.of(1), 0L, tx -> {}, offsets);
    }

    /** What a runner does with the batch after the checkpoint, under {@code mode}. */
    void runLiveBatch(ProjectionDeliveryMode mode) {
      List<EventEnvelope> batch = events.readGlobalStream(GlobalOffset.of(1), 10);
      repo.executeAtomically(
          name,
          batch,
          batch.getLast().globalOffset(),
          0L,
          tx -> projection.process(batch, mode.writesInCheckpointTransaction() ? tx : null),
          offsets);
    }

    ReplayResult replay(ProjectionDeliveryMode mode, ProjectionDeadLetterStore store) {
      return new ProjectionDeadLetterReplayer(events, store, repo)
          .replay(name, projection, mode, 10);
    }

    ProductView row() {
      return repo.findById(name, ROW, ProductView.class).orElseThrow();
    }
  }

  @ParameterizedTest
  @CsvSource({
    "TRANSACTIONAL_LOCAL, AFTER_READ",
    "TRANSACTIONAL_LOCAL, AFTER_WRITE",
    "AT_LEAST_ONCE_IDEMPOTENT, AFTER_READ",
    "AT_LEAST_ONCE_IDEMPOTENT, AFTER_WRITE"
  })
  void replayBesideAnOpenLiveBatch_waitsForTheCheckpointRow_andBothEffectsSurvive(
      ProjectionDeliveryMode mode, HoldPoint holdPoint) throws Exception {
    var fixture = new Fixture();
    var liveIsOpen = new CountDownLatch(1);
    var releaseLive = new CountDownLatch(1);
    Runnable hold =
        () -> {
          liveIsOpen.countDown();
          awaitQuietly(releaseLive);
        };
    if (holdPoint == HoldPoint.AFTER_READ) {
      fixture.projection.afterStockRead = hold;
    } else {
      fixture.projection.afterStockWrite = hold;
    }

    var liveFailure = new AtomicReference<Throwable>();
    var replayOutcome = new AtomicReference<ReplayResult>();
    var replayFailure = new AtomicReference<Throwable>();
    Thread live =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    fixture.runLiveBatch(mode);
                  } catch (Throwable t) {
                    liveFailure.set(t);
                  }
                });
    assertThat(liveIsOpen.await(30, TimeUnit.SECONDS)).as("the live batch opened").isTrue();
    Thread replay =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    replayOutcome.set(fixture.replay(mode, fixture.deadLetters));
                  } catch (Throwable t) {
                    replayFailure.set(t);
                  }
                });
    boolean replayWaited;
    boolean replayReadTheRow;
    try {
      Awaitility.await()
          .atMost(Duration.ofSeconds(30))
          .until(() -> !replay.isAlive() || backendsWaitingForALock() > 0);
      replayWaited = replay.isAlive();
      replayReadTheRow = fixture.projection.priceUpdateEntered.get();
    } finally {
      releaseLive.countDown();
      live.join(30_000);
      replay.join(30_000);
    }

    assertThat(liveFailure.get()).isNull();
    assertThat(replayFailure.get()).isNull();
    assertThat(fixture.row())
        .as("the replayed price and the live stock adjustment both survive")
        .isEqualTo(new ProductView(ROW, 150, 7));
    assertThat(replayWaited)
        .as("the replay waits while the live batch holds the checkpoint row")
        .isTrue();
    assertThat(replayReadTheRow)
        .as("the replay read the row while the live batch that changes it was open")
        .isFalse();
    assertThat(replayOutcome.get()).isEqualTo(new ReplayResult(1, 0, 0));
    assertThat(fixture.deadLetters.read(fixture.name, 10)).isEmpty();
    assertThat(fixture.offsets.getLastOffset(fixture.name))
        .as("the replay does not move the checkpoint")
        .isEqualTo(GlobalOffset.of(2));
  }

  @Test
  void crashBeforeTheReplayCommit_rollsTheRangeBack_keepsTheEntry_andTheNextReplayAppliesItOnce() {
    var fixture = new Fixture();
    fixture.projection.afterPriceWrite =
        () -> CrashItSupport.terminateIdleInTransaction(observerDs, APP);

    ReplayResult crashed = fixture.replay(TRANSACTIONAL_LOCAL, fixture.deadLetters);

    assertThat(crashed).isEqualTo(new ReplayResult(0, 1, 0));
    assertThat(fixture.row())
        .as("the write was inside the terminated transaction")
        .isEqualTo(new ProductView(ROW, 100, 10));
    assertThat(fixture.deadLetters.read(fixture.name, 10)).hasSize(1);
    assertThat(fixture.offsets.getLastOffset(fixture.name)).isEqualTo(GlobalOffset.of(1));

    fixture.projection.afterPriceWrite = () -> {};
    ReplayResult rerun = fixture.replay(TRANSACTIONAL_LOCAL, fixture.deadLetters);

    assertThat(rerun).isEqualTo(new ReplayResult(1, 0, 0));
    assertThat(fixture.row()).isEqualTo(new ProductView(ROW, 150, 10));
    assertThat(fixture.deadLetters.read(fixture.name, 10)).isEmpty();
    assertThat(fixture.offsets.getLastOffset(fixture.name)).isEqualTo(GlobalOffset.of(1));
  }

  @Test
  void crashAfterTheReplayCommit_beforeTheDiscard_keepsTheEntry_andTheNextReplayConverges() {
    var fixture = new Fixture();
    var failingOnce = new FailingOnceDiscard(fixture.deadLetters);

    ReplayResult crashed = fixture.replay(TRANSACTIONAL_LOCAL, failingOnce);

    assertThat(crashed).isEqualTo(new ReplayResult(0, 1, 0));
    assertThat(fixture.row())
        .as("the replay committed before the discard failed")
        .isEqualTo(new ProductView(ROW, 150, 10));
    assertThat(fixture.deadLetters.read(fixture.name, 10)).hasSize(1);

    ReplayResult rerun = fixture.replay(TRANSACTIONAL_LOCAL, failingOnce);

    assertThat(rerun).isEqualTo(new ReplayResult(1, 0, 0));
    assertThat(fixture.row())
        .as("the idempotent price update applied twice leaves the same row")
        .isEqualTo(new ProductView(ROW, 150, 10));
    assertThat(fixture.deadLetters.read(fixture.name, 10)).isEmpty();
    assertThat(fixture.offsets.getLastOffset(fixture.name)).isEqualTo(GlobalOffset.of(1));
  }

  @Test
  void atLeastOnceReplay_runsUnderTheCheckpointRow_andWritesThroughTheProjectionsOwnRepository() {
    var fixture = new Fixture();

    ReplayResult result = fixture.replay(AT_LEAST_ONCE_IDEMPOTENT, fixture.deadLetters);

    assertThat(result).isEqualTo(new ReplayResult(1, 0, 0));
    assertThat(fixture.row()).isEqualTo(new ProductView(ROW, 150, 10));
    assertThat(fixture.deadLetters.read(fixture.name, 10)).isEmpty();
    assertThat(fixture.offsets.getLastOffset(fixture.name)).isEqualTo(GlobalOffset.of(1));
  }

  /** Backends of this test's data source that are blocked on a lock another backend holds. */
  private static int backendsWaitingForALock() {
    try (var c = observerDs.getConnection();
        var ps =
            c.prepareStatement(
                "SELECT count(*) FROM pg_stat_activity"
                    + " WHERE application_name = ? AND wait_event_type = 'Lock'")) {
      ps.setString(1, APP);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      if (!latch.await(60, TimeUnit.SECONDS)) {
        throw new IllegalStateException("the live batch was never released");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  /** Fails the first {@code discard} (the process died after the replay committed). */
  private static final class FailingOnceDiscard implements ProjectionDeadLetterStore {
    private final ProjectionDeadLetterStore delegate;
    private boolean failed;

    FailingOnceDiscard(ProjectionDeadLetterStore delegate) {
      this.delegate = delegate;
    }

    @Override
    public void save(ProjectionDeadLetterEntry entry) {
      delegate.save(entry);
    }

    @Override
    public List<ProjectionDeadLetterEntry> read(ProjectionName projectionName, int limit) {
      return delegate.read(projectionName, limit);
    }

    @Override
    public List<ProjectionDeadLetterEntry> readAll(int limit) {
      return delegate.readAll(limit);
    }

    @Override
    public void discard(ProjectionName projectionName, GlobalOffset fromOffset) {
      if (!failed) {
        failed = true;
        throw new IllegalStateException("the process died before the discard");
      }
      delegate.discard(projectionName, fromOffset);
    }

    @Override
    public long countPending() {
      return delegate.countPending();
    }
  }
}
