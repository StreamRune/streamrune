package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.backend.OutboxOptions;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxOrderingViolationException;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.MutableClock;

/**
 * What the shared contract leaves to the SQLite outbox store: claims racing on several relays and
 * connections, the claim lease on the application clock, timestamps from that clock, the batched
 * writes, the same-transaction {@code saveAll}, and the failure taxonomy every SQLite store shares.
 */
class SqliteOutboxStoreTest {

  private static final AggregateType AGG = AggregateType.of("agg");
  private static final Instant T0 = Instant.parse("2026-11-23T10:00:00.123456Z");
  private static final Duration LEASE = Duration.ofMinutes(4);

  @TempDir Path dir;

  private final List<SqliteDatabase> handles = new ArrayList<>();

  @AfterEach
  void closeHandles() {
    handles.forEach(SqliteDatabase::close);
  }

  private SqliteDatabase open(DataSource dataSource) {
    SqliteDatabase database = SqliteDatabase.open(dataSource);
    handles.add(database);
    return database;
  }

  private SqliteDatabase migrated(String file) {
    SqliteDatabase database = open(TestDatabases.dataSource(dir.resolve(file)));
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    return database;
  }

  private static StreamId stream(String id) {
    return StreamId.of(AGG, AggregateId.of(id));
  }

  private static OutboxEntry pending(String id, StreamId stream) {
    return OutboxEntry.pending(OutboxEntryId.of(id), "{\"k\":1}", "Event", stream);
  }

  private static List<String> ids(List<OutboxEntry> entries) {
    return entries.stream().map(e -> e.id().value()).toList();
  }

  // ── Construction ────────────────────────────────────────────────────────────────────────────

  @Test
  void refusesBadConstructorArguments() {
    SqliteDatabase database = migrated("ctor.db");
    Clock clock = Clock.systemUTC();
    var strict = OutboxOrderingMode.STRICT_PER_AGGREGATE;

    assertThatThrownBy(() -> new SqliteOutboxStore(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("database is required");
    assertThatThrownBy(() -> new SqliteOutboxStore(database, null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("claimLease must be positive");
    assertThatThrownBy(() -> new SqliteOutboxStore(database, Duration.ZERO))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("claimLease must be positive");
    assertThatThrownBy(() -> new SqliteOutboxStore(database, Duration.ofSeconds(-1)))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("claimLease must be positive");
    assertThatThrownBy(() -> new SqliteOutboxStore(database, LEASE, null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("orderingMode is required");
    assertThatThrownBy(() -> new SqliteOutboxStore(database, LEASE, strict, null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("clock is required");
  }

  @Test
  void reportsItsLeaseModeAndIdentity() {
    SqliteDatabase database = migrated("identity.db");

    var defaults = new SqliteOutboxStore(database);
    var other = new SqliteOutboxStore(database);

    assertThat(defaults.claimLease()).isEqualTo(OutboxOptions.DEFAULT_CLAIM_LEASE);
    assertThat(defaults.orderingMode()).isEqualTo(OutboxOrderingMode.STRICT_PER_AGGREGATE);
    assertThat(new SqliteOutboxStore(database, Duration.ofSeconds(30)).claimLease())
        .isEqualTo(Duration.ofSeconds(30));
    assertThat(defaults.claimedBy()).startsWith("outbox-").isNotEqualTo(other.claimedBy());
  }

  @Test
  void refusesBadCallArguments() {
    var store = new SqliteOutboxStore(migrated("args.db"));
    OutboxEntryId id = OutboxEntryId.of("x");

    assertThatThrownBy(() -> store.save(null)).hasMessage("entry is required");
    assertThatThrownBy(() -> store.loadPending(0)).hasMessage("limit must be >= 1");
    assertThatThrownBy(() -> store.markDelivered(null, "me")).hasMessage("id is required");
    assertThatThrownBy(() -> store.markFailed(null, 1, "e", "me")).hasMessage("id is required");
    assertThatThrownBy(() -> store.markRetry(null, 1, "e", Duration.ZERO, "me"))
        .hasMessage("id is required");
    assertThatThrownBy(() -> store.markRetry(id, 1, "e", null, "me"))
        .hasMessage("backoff is required");
    assertThatThrownBy(() -> store.delete(null)).hasMessage("id is required");
    assertThatThrownBy(() -> store.findById(null)).hasMessage("id is required");
    assertThatThrownBy(() -> store.findByStatus(null, 1)).hasMessage("status is required");
    assertThatThrownBy(() -> store.findByStatus(OutboxStatus.PENDING, 0))
        .hasMessage("limit must be >= 1");
    assertThatThrownBy(() -> store.countByStatus(null)).hasMessage("status is required");
    assertThatThrownBy(() -> store.resetFailedToPending(null)).hasMessage("id is required");
    assertThatThrownBy(() -> store.skipFailed(null, "ops", "r")).hasMessage("id is required");
    assertThatThrownBy(() -> store.skipFailed(id, null, "r")).hasMessage("skippedBy is required");
    assertThatThrownBy(() -> store.skipFailed(id, "ops", null)).hasMessage("reason is required");
    assertThatThrownBy(() -> store.deleteSkipped(null)).hasMessage("cutoff is required");
    assertThatThrownBy(() -> store.deleteDelivered(null))
        .isExactlyInstanceOf(NullPointerException.class);
    assertThat(store.markDeliveredAll(List.of(), "me")).isZero();
    assertThat(store.releaseClaims(List.of(), "me")).isZero();
  }

  // ── Claims ──────────────────────────────────────────────────────────────────────────────────

  /**
   * Relays on handles and connections of their own claim at once, round after round: no entry is
   * handed to two of them, every entry is handed out, and no stream has two entries in flight.
   */
  @Test
  @Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void concurrentRelaysNeverClaimTheSameEntry() throws Exception {
    SqliteDatabase database = migrated("race.db");
    int relays = 6;
    List<SqliteOutboxStore> stores = new ArrayList<>();
    for (int r = 0; r < relays; r++) {
      SqliteDatabase handle = open(TestDatabases.dataSource(database.file()));
      stores.add(new SqliteOutboxStore(handle, LEASE, OutboxOrderingMode.AVAILABILITY_FIRST));
    }
    ExecutorService pool = Executors.newFixedThreadPool(relays);
    try {
      for (int round = 0; round < 5; round++) {
        var writer = stores.getFirst();
        int streams = 40;
        for (int i = 0; i < streams; i++) {
          writer.save(pending("r" + round + "-s" + i, stream("r" + round + "-s" + i)));
          writer.save(OutboxEntry.pending(OutboxEntryId.of("r" + round + "-free" + i), "{}", "E"));
        }
        CyclicBarrier start = new CyclicBarrier(relays);
        ConcurrentLinkedQueue<String> claimed = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();
        for (SqliteOutboxStore relay : stores) {
          futures.add(
              pool.submit(
                  () -> {
                    start.await();
                    for (int poll = 0; poll < 10; poll++) {
                      relay.loadPending(5).forEach(e -> claimed.add(e.id().value()));
                    }
                    return null;
                  }));
        }
        for (Future<?> future : futures) {
          future.get();
        }

        List<String> all = new ArrayList<>(claimed);
        assertThat(new HashSet<>(all)).as("round " + round).hasSize(all.size());
        assertThat(all).hasSize(2 * streams);
        assertThat(writer.countByStatus(OutboxStatus.IN_PROGRESS)).isEqualTo(2L * streams);
        assertThat(writer.countByStatus(OutboxStatus.PENDING)).isZero();
        for (SqliteOutboxStore relay : stores) {
          relay
              .findByStatus(OutboxStatus.IN_PROGRESS, 1000)
              .forEach(e -> relay.markDelivered(e.id(), relay.claimedBy()));
        }
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /** Several relays claim one stream's entries at once: exactly one holds its head at a time. */
  @Test
  @Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void concurrentRelaysHoldAtMostOneEntryOfAStream() throws Exception {
    SqliteDatabase database = migrated("one-stream.db");
    int relays = 6;
    List<SqliteOutboxStore> stores = new ArrayList<>();
    for (int r = 0; r < relays; r++) {
      stores.add(new SqliteOutboxStore(open(TestDatabases.dataSource(database.file()))));
    }
    ExecutorService pool = Executors.newFixedThreadPool(relays);
    try {
      StreamId only = stream("only");
      for (int round = 0; round < 5; round++) {
        stores.getFirst().save(pending("h" + round, only));
        stores.getFirst().save(pending("n" + round, only));
        CyclicBarrier start = new CyclicBarrier(relays);
        ConcurrentLinkedQueue<String> claimed = new ConcurrentLinkedQueue<>();
        List<Future<?>> futures = new ArrayList<>();
        for (SqliteOutboxStore relay : stores) {
          futures.add(
              pool.submit(
                  () -> {
                    start.await();
                    relay.loadPending(10).forEach(e -> claimed.add(e.id().value()));
                    return null;
                  }));
        }
        for (Future<?> future : futures) {
          future.get();
        }

        assertThat(claimed).as("round " + round).containsExactly("h" + round);
        OutboxEntryId head = OutboxEntryId.of("h" + round);
        int delivered = 0;
        for (SqliteOutboxStore relay : stores) {
          delivered += relay.markDelivered(head, relay.claimedBy()) ? 1 : 0;
        }
        assertThat(delivered).as("only the claim's own relay records the delivery").isEqualTo(1);
        SqliteOutboxStore next = stores.getFirst();
        assertThat(ids(next.loadPending(10))).containsExactly("n" + round);
        assertThat(next.markDelivered(OutboxEntryId.of("n" + round), next.claimedBy())).isTrue();
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * The lease runs on the application clock: a claim is honoured up to and including the lease,
   * reclaimed by another relay's next claim once the clock is past it, and the relay that lost it
   * records nothing.
   */
  @Test
  void anExpiredClaimIsReclaimedOnTheApplicationClock() throws Exception {
    SqliteDatabase database = migrated("lease.db");
    MutableClock clock = MutableClock.startingAt(T0);
    var first =
        new SqliteOutboxStore(database, LEASE, OutboxOrderingMode.STRICT_PER_AGGREGATE, clock);
    var second =
        new SqliteOutboxStore(database, LEASE, OutboxOrderingMode.STRICT_PER_AGGREGATE, clock);
    first.save(pending("e1", stream("a")));

    assertThat(ids(first.loadPending(10))).containsExactly("e1");
    assertThat(column(database, "e1", "claimed_at")).isEqualTo(SqliteTimestamps.toMicros(T0));
    assertThat(column(database, "e1", "claimed_by")).isEqualTo(first.claimedBy());

    clock.advance(LEASE);
    assertThat(second.loadPending(10)).as("held up to the lease").isEmpty();

    clock.advance(Duration.ofNanos(1000));
    assertThat(ids(second.loadPending(10))).as("reclaimed past the lease").containsExactly("e1");
    assertThat(column(database, "e1", "claimed_by")).isEqualTo(second.claimedBy());

    assertThat(first.markDelivered(OutboxEntryId.of("e1"), first.claimedBy()))
        .as("the relay whose lease was stolen records nothing")
        .isFalse();
    assertThat(second.markDelivered(OutboxEntryId.of("e1"), second.claimedBy())).isTrue();
  }

  /** Every timestamp is the store clock's, and a backoff is a duration on that clock. */
  @Test
  void timestampsAndBackoffAreTheApplicationClocks() throws Exception {
    SqliteDatabase database = migrated("clock.db");
    MutableClock clock = MutableClock.startingAt(T0);
    var store =
        new SqliteOutboxStore(database, LEASE, OutboxOrderingMode.STRICT_PER_AGGREGATE, clock);
    store.save(pending("e1", stream("a")));
    store.save(pending("e2", stream("b")));

    assertThat(store.findById(OutboxEntryId.of("e1")).orElseThrow().createdAt()).isEqualTo(T0);
    assertThat(ids(store.loadPending(10))).containsExactly("e1", "e2");

    assertThat(
            store.markRetry(
                OutboxEntryId.of("e1"), 1, "down", Duration.ofSeconds(30), store.claimedBy()))
        .isTrue();
    var retried = store.findById(OutboxEntryId.of("e1")).orElseThrow();
    assertThat(retried.nextRetryAt()).isEqualTo(T0.plusSeconds(30));
    assertThat(retried.attempts()).isEqualTo(1);
    assertThat(retried.lastError()).isEqualTo("down");

    clock.advance(Duration.ofSeconds(29));
    assertThat(store.loadPending(10)).as("still backing off").isEmpty();
    clock.advance(Duration.ofSeconds(1));
    assertThat(ids(store.loadPending(10))).as("eligible at next_retry_at").containsExactly("e1");

    clock.advance(Duration.ofSeconds(5));
    assertThat(store.markDelivered(OutboxEntryId.of("e1"), store.claimedBy())).isTrue();
    assertThat(store.findById(OutboxEntryId.of("e1")).orElseThrow().processedAt())
        .isEqualTo(T0.plusSeconds(35));
    assertThat(store.markFailed(OutboxEntryId.of("e2"), 3, "poison", store.claimedBy())).isTrue();
    clock.advance(Duration.ofSeconds(1));
    assertThat(store.skipFailed(OutboxEntryId.of("e2"), "ops", "known")).isTrue();
    var skipped = store.findById(OutboxEntryId.of("e2")).orElseThrow();
    assertThat(skipped.processedAt()).isEqualTo(T0.plusSeconds(35));
    assertThat(skipped.skip().skippedAt()).isEqualTo(T0.plusSeconds(36));
    assertThat(skipped.skip().skippedBy()).isEqualTo("ops");
    assertThat(skipped.skip().reason()).isEqualTo("known");
  }

  @Test
  void aBackoffBeyondTheTimestampRangeSaturates() {
    SqliteDatabase database = migrated("saturate.db");
    var store = new SqliteOutboxStore(database, Duration.ofSeconds(Long.MAX_VALUE));
    store.save(pending("e1", stream("a")));
    assertThat(ids(store.loadPending(10))).containsExactly("e1");

    assertThat(
            store.markRetry(
                OutboxEntryId.of("e1"),
                1,
                "x",
                Duration.ofSeconds(Long.MAX_VALUE),
                store.claimedBy()))
        .isTrue();
    assertThat(store.loadPending(10)).as("backing off for ever").isEmpty();

    store.save(pending("e2", stream("b")));
    assertThat(ids(store.loadPending(10))).containsExactly("e2");
    assertThat(
            store.markRetry(
                OutboxEntryId.of("e2"),
                1,
                "x",
                Duration.ofSeconds(Long.MIN_VALUE),
                store.claimedBy()))
        .isTrue();
    assertThat(ids(store.loadPending(10)))
        .as("a huge negative backoff: eligible now")
        .containsExactly("e2");
  }

  // ── Batches ─────────────────────────────────────────────────────────────────────────────────

  /** Set-based marks and retention cover more ids than one statement binds. */
  @Test
  void batchedWritesCoverMoreEntriesThanOneStatement() throws Exception {
    SqliteDatabase database = migrated("batches.db");
    MutableClock clock = MutableClock.startingAt(T0);
    var store =
        new SqliteOutboxStore(database, LEASE, OutboxOrderingMode.AVAILABILITY_FIRST, clock);
    int n = 2_300;
    List<OutboxEntry> entries = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      entries.add(OutboxEntry.pending(OutboxEntryId.of("b" + i), "{}", "E"));
    }
    try (Connection conn = database.dataSource().getConnection()) {
      SqliteTransactions.inTransaction(
          conn,
          "seed",
          c -> {
            store.saveAll(entries, c);
            return null;
          });
    }
    List<OutboxEntry> claimed = store.loadPending(n);
    assertThat(claimed).hasSize(n);
    assertThat(ids(claimed)).isEqualTo(ids(store.findByStatus(OutboxStatus.IN_PROGRESS, n)));

    assertThat(store.releaseClaims(claimed.subList(0, 1_100), store.claimedBy())).isEqualTo(1_100);
    assertThat(store.releaseClaims(claimed.subList(0, 1_100), "someone-else")).isZero();
    assertThat(store.countByStatus(OutboxStatus.PENDING)).isEqualTo(1_100);

    List<OutboxEntryId> inFlight = claimed.subList(1_100, n).stream().map(OutboxEntry::id).toList();
    assertThat(store.markDeliveredAll(inFlight, "someone-else")).isZero();
    assertThat(store.markDeliveredAll(inFlight, store.claimedBy())).isEqualTo(n - 1_100);

    clock.advance(Duration.ofSeconds(1));
    assertThat(store.deleteDelivered(T0)).as("strictly before the cutoff").isZero();
    assertThat(store.deleteDelivered(T0.plusNanos(1_000))).isEqualTo(n - 1_100);
    assertThat(store.countByStatus(OutboxStatus.DELIVERED)).isZero();
  }

  /** The release leaves the retry ladder alone. */
  @Test
  void releaseClaimsTouchesOnlyTheClaim() {
    SqliteDatabase database = migrated("release.db");
    var store = new SqliteOutboxStore(database);
    store.save(pending("e1", stream("a")));
    store.loadPending(1);
    store.markRetry(OutboxEntryId.of("e1"), 2, "blip", Duration.ofSeconds(-1), store.claimedBy());
    List<OutboxEntry> claimed = store.loadPending(1);

    assertThat(store.releaseClaims(claimed, store.claimedBy())).isEqualTo(1);

    var released = store.findById(OutboxEntryId.of("e1")).orElseThrow();
    assertThat(released.status()).isEqualTo(OutboxStatus.PENDING);
    assertThat(released.attempts()).isEqualTo(2);
    assertThat(released.lastError()).isEqualTo("blip");
    assertThat(released.nextRetryAt()).isNotNull();
  }

  @Test
  void aDuplicateSaveIsIgnored() {
    var store = new SqliteOutboxStore(migrated("dup.db"));
    store.save(pending("e1", stream("a")));
    store.save(OutboxEntry.pending(OutboxEntryId.of("e1"), "{\"other\":2}", "Other", stream("b")));

    var stored = store.findById(OutboxEntryId.of("e1")).orElseThrow();
    assertThat(stored.payloadType()).isEqualTo("Event");
    assertThat(stored.streamId()).isEqualTo(stream("a"));
    assertThat(store.countByStatus(OutboxStatus.PENDING)).isEqualTo(1);
  }

  // ── saveAll, on the caller's connection ─────────────────────────────────────────────────────

  /** saveAll joins the caller's transaction: it neither commits nor rolls back. */
  @Test
  void saveAllWritesInTheCallersTransaction() throws Exception {
    SqliteDatabase database = migrated("saveall.db");
    var store = new SqliteOutboxStore(database);
    try (Connection conn = database.dataSource().getConnection()) {
      SqliteTransactions.begin(conn);
      store.saveAll(List.of(pending("e1", stream("a")), pending("e2", stream("a"))), conn);
      SqliteTransactions.rollback(conn, "test");
    }
    assertThat(store.countByStatus(OutboxStatus.PENDING))
        .as("rolled back with the caller")
        .isZero();

    try (Connection conn = database.dataSource().getConnection()) {
      SqliteTransactions.begin(conn);
      store.saveAll(List.of(pending("e1", stream("a")), pending("e2", stream("a"))), conn);
      store.saveAll(List.of(), conn);
      SqliteTransactions.commit(conn);
    }
    assertThat(ids(store.findByStatus(OutboxStatus.PENDING, 10))).containsExactly("e1", "e2");
  }

  @Test
  void saveAllOnAStrictChannelRefusesAnEntryWithoutAStreamBeforeAnyWrite() throws Exception {
    SqliteDatabase database = migrated("saveall-strict.db");
    var store = new SqliteOutboxStore(database);
    try (Connection conn = database.dataSource().getConnection()) {
      SqliteTransactions.begin(conn);
      assertThatThrownBy(
              () ->
                  store.saveAll(
                      List.of(
                          pending("e1", stream("a")),
                          OutboxEntry.pending(OutboxEntryId.of("free"), "{}", "E")),
                      conn))
          .isExactlyInstanceOf(OutboxOrderingViolationException.class);
      assertThat(countRows(conn)).as("nothing written, not even the first entry").isZero();
      SqliteTransactions.commit(conn);
    }
    assertThatThrownBy(() -> store.saveAll(null, null)).hasMessage("entries is required");
    assertThatThrownBy(() -> store.saveAll(List.of(), null)).hasMessage("conn is required");
  }

  @Test
  void saveAllWrapsAStorageFailure() throws Exception {
    SqliteDatabase database = migrated("saveall-fail.db");
    var store = new SqliteOutboxStore(database);
    try (Connection conn = database.dataSource().getConnection()) {
      SqliteTransactions.begin(conn);
      try (Statement st = conn.createStatement()) {
        st.execute("DROP TABLE outbox_events");
      }
      assertThatThrownBy(() -> store.saveAll(List.of(pending("e1", stream("a"))), conn))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage("Failed to save 1 outbox entries")
          .hasCauseInstanceOf(SQLException.class);
      SqliteTransactions.rollback(conn, "test");
    }
  }

  // ── Reads ───────────────────────────────────────────────────────────────────────────────────

  /** A stored trace id that is not a W3C id reads back as no link, and the entry still delivers. */
  @Test
  void aCorruptStoredTraceLinkReadsBackAsNone() throws Exception {
    SqliteDatabase database = migrated("trace.db");
    var store = new SqliteOutboxStore(database);
    store.save(pending("e1", stream("a")));
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute("UPDATE outbox_events SET trace_id = 'zz', span_id = 'yy'");
    }

    assertThat(store.findById(OutboxEntryId.of("e1")).orElseThrow().traceLink())
        .isSameAs(SpanLink.NONE);
    assertThat(store.loadPending(1).getFirst().traceLink()).isSameAs(SpanLink.NONE);
  }

  /** A claimed entry reports no processed time, even a replayed one that carries its FAILED's. */
  @Test
  void aClaimedEntryReportsNoProcessedTime() {
    var store = new SqliteOutboxStore(migrated("claimed.db"));
    store.save(pending("e1", stream("a")));
    store.loadPending(1);
    store.markFailed(OutboxEntryId.of("e1"), 1, "p", store.claimedBy());
    store.resetFailedToPending(OutboxEntryId.of("e1"));

    var claimed = store.loadPending(1).getFirst();

    assertThat(claimed.processedAt()).isNull();
    assertThat(claimed.status()).isEqualTo(OutboxStatus.IN_PROGRESS);
    assertThat(store.findById(OutboxEntryId.of("e1")).orElseThrow().processedAt()).isNotNull();
  }

  // ── Failures ────────────────────────────────────────────────────────────────────────────────

  /** A storage failure is an EventStoreException with Postgres' message, the cause kept. */
  @Test
  void storageFailuresAreEventStoreExceptions() {
    SwitchableDataSource source =
        new SwitchableDataSource(TestDatabases.dataSource(dir.resolve("broken.db")));
    SqliteDatabase database = open(source);
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    var store = new SqliteOutboxStore(database);
    source.failWith(() -> new IllegalStateException("pool closed"));
    OutboxEntryId id = OutboxEntryId.of("e1");

    assertFails(() -> store.save(pending("e1", stream("a"))), "Failed to save outbox entry: e1");
    assertFails(() -> store.loadPending(1), "Failed to load pending outbox entries");
    assertFails(
        () -> store.markDelivered(id, "me"), "Failed to mark outbox entry as delivered: e1");
    assertFails(
        () -> store.markFailed(id, 1, "e", "me"), "Failed to mark outbox entry as failed: e1");
    assertFails(
        () -> store.markRetry(id, 1, "e", Duration.ZERO, "me"),
        "Failed to mark outbox entry for retry: e1");
    assertFails(
        () -> store.markDeliveredAll(List.of(id), "me"), "Failed to mark outbox entries delivered");
    assertFails(
        () -> store.releaseClaims(List.of(pending("e1", stream("a"))), "me"),
        "Failed to release unattempted outbox claims");
    assertFails(() -> store.delete(id), "Failed to delete outbox entry: e1");
    assertFails(() -> store.deleteDelivered(T0), "Failed to delete delivered outbox entries");
    assertFails(() -> store.deleteSkipped(T0), "Failed to delete skipped outbox entries");
    assertFails(
        () -> store.findByStatus(OutboxStatus.FAILED, 1),
        "Failed to find outbox entries by status: FAILED");
    assertFails(() -> store.findById(id), "Failed to find outbox entry: e1");
    assertFails(
        () -> store.countByStatus(OutboxStatus.FAILED),
        "Failed to count outbox entries by status: FAILED");
    assertFails(
        () -> store.resetFailedToPending(id), "Failed to reset failed outbox entry to pending: e1");
    assertFails(() -> store.skipFailed(id, "ops", "r"), "Failed to skip failed outbox entry: e1");
    assertFails(store::sampleBlockage, "Failed to sample outbox blockage");
  }

  private static void assertFails(Runnable call, String message) {
    assertThatThrownBy(call::run)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(message)
        .hasRootCauseInstanceOf(IllegalStateException.class);
  }

  /** A connection in manual-commit mode is refused unwrapped, before anything is written. */
  @Test
  void aManualCommitConnectionIsRefusedUnwrapped() {
    SwitchableDataSource source =
        new SwitchableDataSource(TestDatabases.dataSource(dir.resolve("manual.db")));
    SqliteDatabase database = open(source);
    new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry()).create();
    var store = new SqliteOutboxStore(database);
    source.manualCommit();

    assertThatThrownBy(() -> store.save(pending("e1", stream("a"))))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessage(SqliteTransactions.MANUAL_COMMIT_CONNECTION);
    assertThatThrownBy(() -> store.loadPending(1)).isExactlyInstanceOf(IllegalStateException.class);
  }

  /** A write that does not get the write lock within busy_timeout says so and stores nothing. */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aWriteThatWaitsLongerThanBusyTimeoutSaysSo() throws Exception {
    Path file = dir.resolve("busy.db");
    migrated("busy.db");
    SqliteDatabase impatient = open(TestDatabases.dataSource(file, "busy_timeout=50"));
    var store = new SqliteOutboxStore(impatient);
    try (Connection holder = TestDatabases.dataSource(file).getConnection()) {
      SqliteTransactions.begin(holder);
      assertThatThrownBy(() -> store.save(pending("e1", stream("a"))))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageContaining("busy_timeout");
      SqliteTransactions.rollback(holder, "test");
    }
    assertThat(store.countByStatus(OutboxStatus.PENDING)).isZero();
  }

  // ── Plans ───────────────────────────────────────────────────────────────────────────────────

  /**
   * The claim runs under the database-wide write lock on every poll, so it must not read the whole
   * table (the DELIVERED backlog included): in both modes, and for the reclaim before it, every
   * read of outbox_events goes through an index.
   */
  @Test
  void theClaimReadsOutboxEventsThroughIndexesOnly() throws Exception {
    SqliteDatabase database = migrated("plan.db");
    for (OutboxOrderingMode mode : OutboxOrderingMode.values()) {
      List<String> plan = plan(database, SqliteOutboxStore.claimPendingSql(mode));
      assertThat(plan).as(mode + ": " + plan).noneMatch(SqliteOutboxStoreTest::isFullScan);
      assertThat(plan).as(mode + ": " + plan).anyMatch(line -> line.contains("USING"));
    }
    List<String> reclaim = plan(database, SqliteOutboxStore.reclaimExpiredSql());
    assertThat(reclaim).as(reclaim.toString()).noneMatch(SqliteOutboxStoreTest::isFullScan);
  }

  /** A plan line that reads a table of outbox_events row by row, without an index. */
  private static boolean isFullScan(String line) {
    return line.matches("SCAN (outbox_events|o|ip)( |$).*") && !line.contains("USING");
  }

  private static List<String> plan(SqliteDatabase database, String sql) throws SQLException {
    List<String> lines = new ArrayList<>();
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("EXPLAIN QUERY PLAN " + sql)) {
      while (rs.next()) {
        lines.add(rs.getString("detail"));
      }
    }
    return lines;
  }

  // ── helpers ─────────────────────────────────────────────────────────────────────────────────

  private static Object column(SqliteDatabase database, String id, String column)
      throws SQLException {
    try (Connection conn = database.dataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT " + column + " FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getObject(1) instanceof Integer i ? Long.valueOf(i) : rs.getObject(1);
      }
    }
  }

  private static long countRows(Connection conn) throws SQLException {
    try (Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT count(*) FROM outbox_events")) {
      rs.next();
      return rs.getLong(1);
    }
  }
}
