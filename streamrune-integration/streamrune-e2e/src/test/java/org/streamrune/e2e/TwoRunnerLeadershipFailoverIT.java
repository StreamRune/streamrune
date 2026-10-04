package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.postgres.LeaseBasedLeadership;
import org.streamrune.postgres.PostgresOffsetStore;

/**
 * The definitive two-runner failover proof for the epoch-fenced lease (Part B): a zombie old leader
 * that resumes after a newer leader has taken over is rejected <b>by the database</b> at the write
 * fence, not merely by an in-JVM leadership check.
 *
 * <p>This composes the two real primitives against one shared PostgreSQL: a DB-clocked,
 * epoch-issuing {@link LeaseBasedLeadership} lease over {@code subscription_leases}, and the epoch
 * write-fence in {@link JdbcProjectionRepository#executeAtomically} over {@code projection_offset}.
 * Rather than drive a full {@link org.streamrune.runtime.ContinuousProjectionRunner} (whose
 * failover liveness is proven under load by {@code ProjectionCrashRecoveryIT} / {@code
 * HaDualInstanceProjectionIT}), each leader's commit is issued directly through {@code
 * executeAtomically} carrying its held fencing epoch — exactly what the runner threads into every
 * commit — so the fence seam is exercised deterministically, with no timing dependence on batch
 * draining.
 *
 * <p><b>What is proven (non-vacuous):</b>
 *
 * <ul>
 *   <li><b>Takeover at epoch+1:</b> a first acquisition yields epoch 1; after the leader resigns
 *       (its lease expires), the standby's takeover of the expired lease bumps the fencing epoch by
 *       exactly one. If the epoch did not advance, the fence downstream could never distinguish the
 *       zombie from the new leader.
 *   <li><b>Stamp on the first advance:</b> at the raw repository layer the checkpoint carries the
 *       new leader's epoch only after its <em>first advancing commit</em> — before that first
 *       advance it still carries the old epoch. This pins <em>when</em> the commit-path stamp
 *       engages. The runners no longer rely on it alone: they stamp the acquired epoch at takeover
 *       via {@code stampFencingEpoch}, which the third test proves fences the zombie
 *       <em>before</em> the new leader's first commit.
 *   <li><b>Zombie commit fenced by the DB:</b> once the new leader has stamped, a commit issued
 *       under the stale epoch is rejected with {@link OptimisticLockException} <em>before it writes
 *       a single read-model row</em>. The offset does not regress and the read model is untouched
 *       by the zombie.
 *   <li><b>Exactly-once:</b> every legitimately-committed offset lands in the read model exactly
 *       once ({@code applyCount == 1}); no offset is applied twice and none is lost, and the fenced
 *       offset is absent entirely.
 *   <li><b>Genuine contention:</b> when a stale commit and a fresh commit race concurrently through
 *       the checkpoint {@code FOR UPDATE} lock — both individually able to pass the
 *       overlap/monotonic guards — exactly one of the two legal interleavings occurs, and the final
 *       state is consistent regardless of which thread won: the new leader's write is present,
 *       nothing is double-applied, and the stamped epoch is the new leader's. The test asserts the
 *       invariant, never a fixed winner.
 * </ul>
 */
@Timeout(120)
class TwoRunnerLeadershipFailoverIT extends E2ETestBase {

  private static final ProjectionName PROJECTION = ProjectionName.of("failover_projection");
  private static final String CONSUMER = PROJECTION.value();
  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("failover"), AggregateId.of("failover-stream"));

  /** A short lease so the dying leader's lease is up for grabs promptly after it resigns. */
  private static final Duration SHORT_TTL = Duration.ofMillis(400);

  /** A long lease for the survivor so it is not itself at risk of takeover mid-test. */
  private static final Duration LONG_TTL = Duration.ofSeconds(30);

  /**
   * {@link E2ETestBase#cleanDatabase()} does not touch {@code subscription_leases}, so a lease row
   * left by a previous test method would make the next acquisition a <em>takeover</em> (epoch &gt;
   * 1) instead of a fresh insert. Clear it so epoch numbering is hermetic per test. Scoped to this
   * IT — no other e2e class inherits this hook.
   */
  @BeforeEach
  void clearLeases() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM subscription_leases");
    }
  }

  @Test
  void leaderDies_standbyTakesOverAtNextEpoch_oldLeaderInFlightCommitRejected() throws Exception {
    JdbcProjectionRepository repo = new JdbcProjectionRepository(dataSource);
    PostgresOffsetStore offsetStore = new PostgresOffsetStore(dataSource);
    OffsetCountingProjection projection = new OffsetCountingProjection(PROJECTION);

    LeaseBasedLeadership leadershipA = new LeaseBasedLeadership(dataSource, SHORT_TTL);
    LeaseBasedLeadership leadershipB = new LeaseBasedLeadership(dataSource, LONG_TTL);
    try {
      // 1. Leader A acquires the name → epoch a (== 1) and commits event 1 under it. Its advancing
      //    commit stamps epoch a onto the checkpoint (GREATEST(0, a)).
      long epochA = leadershipA.tryAcquire(CONSUMER).orElseThrow().epoch();
      assertThat(epochA).as("a first-ever acquisition yields fencing epoch 1").isEqualTo(1L);

      commit(repo, projection, offsetStore, /* offset= */ 1L, /* n= */ 0, epochA);
      assertThat(offsetStore.getLastOffset(PROJECTION).value()).isEqualTo(1L);

      // 2. A "dies": close() resigns its lease (expires it) and stops its renew heartbeat. The
      //    standby B takes over the expired lease at the NEXT epoch. Awaitility (not sleep) so the
      //    takeover is race-free under any lease/heartbeat timing.
      leadershipA.close();
      await()
          .atMost(Duration.ofSeconds(30))
          .pollInterval(Duration.ofMillis(50))
          .until(() -> leadershipB.tryAcquire(CONSUMER).isPresent());
      long epochB = leadershipB.current(CONSUMER).orElseThrow().epoch();
      assertThat(epochB)
          .as("taking over the expired lease bumps the fencing epoch by exactly one")
          .isEqualTo(epochA + 1);

      // Part 1: the COMMIT-PATH stamp engages only once the NEW leader
      // commits. Until B commits (this test deliberately issues no takeover stamp), the checkpoint
      // still carries A's epoch — a zombie A would NOT yet be fenced here. The runners close this
      // window with the takeover stamp, proven by takeoverStampFencesZombie... below.
      assertThat(storedEpoch())
          .as("before B's first advance the checkpoint still carries the OLD epoch")
          .isEqualTo(epochA);

      // 3. B commits event 2 under epoch b. Its first ADVANCING commit stamps b onto the
      // checkpoint.
      commit(repo, projection, offsetStore, /* offset= */ 2L, /* n= */ 1, epochB);
      assertThat(offsetStore.getLastOffset(PROJECTION).value()).isEqualTo(2L);

      // Part 2: the stamp lands on that first real advance.
      assertThat(storedEpoch())
          .as("B's first advancing commit stamps its epoch onto the checkpoint")
          .isEqualTo(epochB);

      // 4. The ZOMBIE old leader A retries a commit under its STALE epoch a → rejected by the DB
      //    with OptimisticLockException, before it writes a single read-model row.
      assertThatThrownBy(
              () -> commit(repo, projection, offsetStore, /* offset= */ 3L, /* n= */ 2, epochA))
          .as("a stale-epoch commit is fenced by the DB (epoch a < stored epoch b)")
          .isInstanceOf(OptimisticLockException.class);

      // The zombie changed nothing: offset stays at B's advance, the stamped epoch is unchanged.
      assertThat(offsetStore.getLastOffset(PROJECTION).value())
          .as("the zombie's rejected commit did not advance the offset")
          .isEqualTo(2L);
      assertThat(storedEpoch()).as("the zombie did not restamp the checkpoint").isEqualTo(epochB);

      // 5. Exactly-once: only the two legitimate offsets are applied, once each; offset 3 is
      // absent.
      Map<Long, AppliedRow> applied = appliedByOffset(repo);
      assertThat(applied.keySet())
          .as(
              "only the two legitimately-committed offsets are present — the fenced offset 3 is absent")
          .containsExactlyInAnyOrder(1L, 2L);
      assertThat(applied.values())
          .as("every applied event landed exactly once — no split-brain double-apply")
          .allSatisfy(row -> assertThat(row.applyCount()).isEqualTo(1));
      assertThat(applied.get(1L).n()).as("offset 1 carries A's payload").isZero();
      assertThat(applied.get(2L).n()).as("offset 2 carries B's payload").isEqualTo(1);
    } finally {
      leadershipA.close(); // idempotent — already closed in the body
      leadershipB.close();
    }
  }

  @Test
  void takeoverStampFencesZombie_beforeNewLeadersFirstCommit() throws Exception {
    // The two neighboring tests pin the RAW repository semantics — the commit-path
    // stamp lags the takeover, so the fence is inert from B's acquire until B's first committed
    // advance (a window that could span a whole catch-up or an idle cron period). Every
    // leadership-aware runner now closes it by stamping the acquired epoch on the checkpoint row
    // at takeover, before reading anything. This test drives that exact sequence end-to-end
    // against the real lease + real fence: A leads and commits; A dies; B takes over and STAMPS
    // (no commit); the zombie A is fenced by the DB before B has processed a single event.
    JdbcProjectionRepository repo = new JdbcProjectionRepository(dataSource);
    PostgresOffsetStore offsetStore = new PostgresOffsetStore(dataSource);
    OffsetCountingProjection projection = new OffsetCountingProjection(PROJECTION);

    LeaseBasedLeadership leadershipA = new LeaseBasedLeadership(dataSource, SHORT_TTL);
    LeaseBasedLeadership leadershipB = new LeaseBasedLeadership(dataSource, LONG_TTL);
    try {
      long epochA = leadershipA.tryAcquire(CONSUMER).orElseThrow().epoch();
      commit(repo, projection, offsetStore, /* offset= */ 1L, /* n= */ 0, epochA);
      leadershipA.close();

      await()
          .atMost(Duration.ofSeconds(30))
          .pollInterval(Duration.ofMillis(50))
          .until(() -> leadershipB.tryAcquire(CONSUMER).isPresent());
      long epochB = leadershipB.current(CONSUMER).orElseThrow().epoch();
      assertThat(storedEpoch())
          .as("precondition: the takeover window is open — the row still carries A's epoch")
          .isEqualTo(epochA);

      // The takeover stamp — what ContinuousProjectionRunner does right after
      // awaitLeadership and ScheduledProjectionRunner does on every leader tick, BEFORE reading.
      repo.stampFencingEpoch(PROJECTION, epochB);

      assertThat(storedEpoch())
          .as("the takeover stamp lands B's epoch without any commit")
          .isEqualTo(epochB);
      assertThat(offsetStore.getLastOffset(PROJECTION).value())
          .as("the takeover stamp never moves the checkpoint offset")
          .isEqualTo(1L);

      // The zombie is fenced BEFORE B's first commit — the window the fix closes.
      assertThatThrownBy(
              () -> commit(repo, projection, offsetStore, /* offset= */ 2L, /* n= */ 1, epochA))
          .as("a stale-epoch commit is fenced by the DB before the new leader ever commits")
          .isInstanceOf(OptimisticLockException.class);
      assertThat(offsetStore.getLastOffset(PROJECTION).value()).isEqualTo(1L);

      // B's own first advance proceeds normally on top of its takeover stamp.
      commit(repo, projection, offsetStore, /* offset= */ 2L, /* n= */ 1, epochB);
      assertThat(offsetStore.getLastOffset(PROJECTION).value()).isEqualTo(2L);
      assertThat(storedEpoch()).isEqualTo(epochB);

      Map<Long, AppliedRow> applied = appliedByOffset(repo);
      assertThat(applied.keySet())
          .as("only A's pre-takeover write and B's write are present — the zombie's is absent")
          .containsExactlyInAnyOrder(1L, 2L);
      assertThat(applied.values())
          .as("nothing double-applied")
          .allSatisfy(row -> assertThat(row.applyCount()).isEqualTo(1));
    } finally {
      leadershipA.close();
      leadershipB.close();
    }
  }

  @Test
  void staleLeaderCommitRacesNewLeader_exactlyOneLegalInterleaving_finalStateConsistent()
      throws Exception {
    JdbcProjectionRepository repo = new JdbcProjectionRepository(dataSource);
    PostgresOffsetStore offsetStore = new PostgresOffsetStore(dataSource);
    OffsetCountingProjection projection = new OffsetCountingProjection(PROJECTION);

    LeaseBasedLeadership leadershipA = new LeaseBasedLeadership(dataSource, SHORT_TTL);
    LeaseBasedLeadership leadershipB = new LeaseBasedLeadership(dataSource, LONG_TTL);
    try {
      // Seed the checkpoint at (offset 1, epoch a) with A's first commit, then A dies and B takes
      // over at epoch b — but B has NOT yet committed, so the checkpoint still carries epoch a.
      long epochA = leadershipA.tryAcquire(CONSUMER).orElseThrow().epoch();
      assertThat(epochA).isEqualTo(1L);
      commit(repo, projection, offsetStore, /* offset= */ 1L, /* n= */ 0, epochA);
      leadershipA.close();

      await()
          .atMost(Duration.ofSeconds(30))
          .pollInterval(Duration.ofMillis(50))
          .until(() -> leadershipB.tryAcquire(CONSUMER).isPresent());
      long epochB = leadershipB.current(CONSUMER).orElseThrow().epoch();
      assertThat(epochB).isEqualTo(epochA + 1);

      // Precondition for GENUINE contention: with the checkpoint still at epoch a, a stale-A commit
      // (epoch a, offset 2) and a fresh-B commit (epoch b, offset 3) BOTH individually pass the
      // epoch/overlap/monotonic guards. The winner is decided purely by who grabs the checkpoint
      // FOR UPDATE lock first — the race is real, not pre-decided. (With the takeover stamp in
      // place this models the
      // RESIDUAL sliver: a stale transaction that takes the row lock in the instants between B's
      // acquire and B's takeover stamp — the row-lock serialization proven here is exactly what
      // keeps that residual consistent: either the stale write lands wholly before the stamp, or
      // it is fenced.)
      assertThat(storedEpoch())
          .as("checkpoint still carries the old epoch — both commits are individually legal")
          .isEqualTo(epochA);

      // Two threads, one start barrier: A commits offset 2 @ epoch a; B commits offset 3 @ epoch b.
      CountDownLatch start = new CountDownLatch(1);
      AtomicReference<Throwable> errorA = new AtomicReference<>();
      AtomicReference<Throwable> errorB = new AtomicReference<>();
      Thread tA =
          Thread.ofVirtual()
              .start(
                  () -> {
                    awaitBarrier(start);
                    try {
                      commit(repo, projection, offsetStore, /* offset= */ 2L, /* n= */ 1, epochA);
                    } catch (Throwable t) {
                      errorA.set(t);
                    }
                  });
      Thread tB =
          Thread.ofVirtual()
              .start(
                  () -> {
                    awaitBarrier(start);
                    try {
                      commit(repo, projection, offsetStore, /* offset= */ 3L, /* n= */ 2, epochB);
                    } catch (Throwable t) {
                      errorB.set(t);
                    }
                  });
      start.countDown();
      tA.join(Duration.ofSeconds(30).toMillis());
      tB.join(Duration.ofSeconds(30).toMillis());
      assertThat(tA.isAlive()).as("racing commit A finished").isFalse();
      assertThat(tB.isAlive()).as("racing commit B finished").isFalse();

      // ---- INVARIANT (holds for EITHER interleaving) ----
      // The new leader always commits: its epoch dominates and its offset always advances.
      assertThat(errorB.get()).as("the new leader's commit is always legal").isNull();

      Map<Long, AppliedRow> applied = appliedByOffset(repo);
      assertThat(offsetStore.getLastOffset(PROJECTION).value())
          .as("the final offset is the new leader's advance, regardless of interleaving")
          .isEqualTo(3L);
      assertThat(storedEpoch())
          .as("the final stamped epoch is the new leader's, regardless of interleaving")
          .isEqualTo(epochB);
      assertThat(applied).as("the new leader's write is present").containsKey(3L);
      assertThat(applied.values())
          .as("no offset applied twice under contention")
          .allSatisfy(row -> assertThat(row.applyCount()).isEqualTo(1));

      // Exactly ONE legal outcome for the stale leader — asserted as an invariant, not a winner:
      if (errorA.get() == null) {
        // A won the row lock BEFORE B stamped: a legal pre-takeover commit at the lower offset,
        // then
        // B commits on top. Both writes present, neither double-applied.
        assertThat(applied.keySet())
            .as("A committed pre-takeover (won the lock first); all three writes present")
            .containsExactlyInAnyOrder(1L, 2L, 3L);
      } else {
        // B stamped first → A is a proven zombie and is fenced by the DB; its write never landed.
        assertThat(errorA.get())
            .as("if the new leader stamped first, the stale leader is fenced by the DB")
            .isInstanceOf(OptimisticLockException.class);
        assertThat(applied.keySet())
            .as("the fenced stale commit left no read-model row")
            .containsExactlyInAnyOrder(1L, 3L);
      }
    } finally {
      leadershipA.close(); // idempotent — already closed in the body
      leadershipB.close();
    }
  }

  // ---- helpers -------------------------------------------------------------------------------

  /**
   * Issues a single-event commit through the epoch write-fence exactly as {@link
   * org.streamrune.runtime.ContinuousProjectionRunner} does: the batch's offset is both the batch
   * event's offset and the new checkpoint, and {@code epoch} is the caller's held fencing epoch.
   */
  private static void commit(
      JdbcProjectionRepository repo,
      OffsetCountingProjection projection,
      PostgresOffsetStore offsetStore,
      long offset,
      int n,
      long epoch) {
    List<EventEnvelope> batch = List.of(envelope(offset, n));
    repo.executeAtomically(
        PROJECTION,
        batch,
        GlobalOffset.of(offset),
        epoch,
        txRepository -> projection.process(batch, txRepository),
        offsetStore);
  }

  /** A minimal, valid {@link CountEvent} envelope carrying the given global offset and payload. */
  private static EventEnvelope envelope(long offset, int n) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        STREAM,
        new Version(offset),
        new EventType(CountEvent.TYPE),
        new CountEvent(n),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("failover-corr-" + n),
            null,
            null,
            Instant.now()));
  }

  /** Reads the fencing epoch stamped on the projection's checkpoint row (0 if no row yet). */
  private static long storedEpoch() throws SQLException {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT epoch FROM projection_offset WHERE projection_name = ?")) {
      ps.setString(1, PROJECTION.value());
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getLong(1) : 0L;
      }
    }
  }

  /** The read model keyed by global offset, for exactly-once / presence assertions. */
  private static Map<Long, AppliedRow> appliedByOffset(JdbcProjectionRepository repo) {
    return repo.findAll(PROJECTION, AppliedRow.class).stream()
        .collect(Collectors.toMap(AppliedRow::offset, row -> row));
  }

  private static void awaitBarrier(CountDownLatch start) {
    try {
      start.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted waiting for the race start barrier", e);
    }
  }
}
