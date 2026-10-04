package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.subscription.SubscriptionLeadership.Lease;
import org.streamrune.test.MutableClock;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Testcontainers coverage for {@link LeaseBasedLeadership} against a real PostgreSQL migrated to
 * the event-store baseline (which creates {@code subscription_leases}).
 *
 * <p>The correctness core under test: acquire is a single DB-clocked atomic statement (fresh insert
 * → epoch 1; takeover of an expired lease → epoch+1; live lease held elsewhere → no row); {@code
 * resign} EXPIRES rather than DELETEs so epochs stay monotonic across resign→reacquire; {@code
 * current} is a purely local read maintained by the renew heartbeat, and a stolen lease is detected
 * by the failed renew and dropped.
 */
@Testcontainers
class LeaseBasedLeadershipTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_lease_test");

  static PGSimpleDataSource dataSource;

  private final List<SubscriptionLeadership> managed = new ArrayList<>();

  @BeforeAll
  static void migrateToHead() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    Flyway.configure()
        .dataSource(dataSource)
        .locations("classpath:db/streamrune-migration")
        .table("flyway_schema_history_streamrune")
        .baselineOnMigrate(false)
        .load()
        .migrate();
  }

  @BeforeEach
  void cleanLeases() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM subscription_leases");
    }
  }

  @AfterEach
  void closeAll() {
    for (SubscriptionLeadership l : managed) {
      l.close();
    }
    managed.clear();
  }

  private LeaseBasedLeadership leadership(Duration ttl) {
    LeaseBasedLeadership l = new LeaseBasedLeadership(dataSource, ttl);
    managed.add(l);
    return l;
  }

  // ---- helpers ------------------------------------------------------------------------------

  /**
   * Atomically overwrites a lease row the way a second live acquirer would: new holder, +1 epoch.
   */
  private void simulateSteal(String name) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "UPDATE subscription_leases SET holder_id = 'other-replica', "
                    + "epoch = epoch + 1, lease_until = now() + interval '1 hour' "
                    + "WHERE consumer_name = ?")) {
      ps.setString(1, name);
      assertEquals(1, ps.executeUpdate(), "expected exactly one lease row to steal");
    }
  }

  private long dbEpoch(String name) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT epoch FROM subscription_leases WHERE consumer_name = ?")) {
      ps.setString(1, name);
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next(), () -> "no lease row for " + name);
        return rs.getLong(1);
      }
    }
  }

  private long dbRowCount(String name) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT count(*) FROM subscription_leases WHERE consumer_name = ?")) {
      ps.setString(1, name);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  private void awaitCurrentEmpty(SubscriptionLeadership l, String name, long timeoutMillis)
      throws InterruptedException {
    long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;
    while (System.nanoTime() < deadline) {
      if (l.current(name).isEmpty()) {
        return;
      }
      Thread.sleep(10);
    }
    assertTrue(
        l.current(name).isEmpty(), () -> "lease for " + name + " was not dropped within timeout");
  }

  private void awaitTrue(java.util.concurrent.Callable<Boolean> condition, String message)
      throws Exception {
    long deadline = System.nanoTime() + 10_000 * 1_000_000L;
    while (System.nanoTime() < deadline) {
      if (condition.call()) {
        return;
      }
      Thread.sleep(20);
    }
    assertTrue(condition.call(), message);
  }

  /** The lease row's {@code lease_until} as epoch seconds — advances on every successful renew. */
  private double dbLeaseUntilEpochSeconds(String name) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT extract(epoch from lease_until) FROM subscription_leases"
                    + " WHERE consumer_name = ?")) {
      ps.setString(1, name);
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next(), () -> "no lease row for " + name);
        return rs.getDouble(1);
      }
    }
  }

  // ---- tests --------------------------------------------------------------------------------

  @Test
  void acquireOnAbsentLease_returnsEpochOne() {
    var a = leadership(Duration.ofSeconds(30));
    Optional<Lease> lease = a.tryAcquire("p1");
    assertTrue(lease.isPresent());
    assertEquals(1L, lease.get().epoch());
    assertEquals(1L, a.current("p1").orElseThrow().epoch());
  }

  @Test
  void secondHolderGetsEmptyWhileLeaseLive() {
    var a = leadership(Duration.ofSeconds(30));
    var b = leadership(Duration.ofSeconds(30));
    assertEquals(1L, a.tryAcquire("p2").orElseThrow().epoch());
    assertTrue(b.tryAcquire("p2").isEmpty(), "second holder must not steal a live lease");
    assertEquals(1L, a.current("p2").orElseThrow().epoch());
    assertTrue(b.current("p2").isEmpty());
  }

  @Test
  void expiredLeaseTakenOverWithEpochBump() throws Exception {
    var a = leadership(Duration.ofSeconds(30));
    assertEquals(1L, a.tryAcquire("p3").orElseThrow().epoch());
    a.close(); // resign EXPIRES the lease (lease_until = now()) and stops A's heartbeat
    Thread.sleep(10); // let now() advance past the expiry stamp
    var b = leadership(Duration.ofSeconds(30));
    Optional<Lease> takeover = b.tryAcquire("p3");
    assertTrue(takeover.isPresent(), "B must take over the expired lease");
    assertEquals(2L, takeover.get().epoch(), "takeover of an expired lease bumps the epoch");
  }

  @Test
  void renewKeepsSameEpoch_currentStaysLive() throws Exception {
    var a = leadership(Duration.ofMillis(300)); // heartbeat interval ~100ms
    assertEquals(1L, a.tryAcquire("p4").orElseThrow().epoch());
    Thread.sleep(700); // spans several heartbeat renews
    assertEquals(1L, a.current("p4").orElseThrow().epoch(), "renew must not drop the lease");
    assertEquals(1L, dbEpoch("p4"), "renew must not bump the epoch");
  }

  @Test
  void loserFencedOut_currentBecomesEmptyAfterSteal() throws Exception {
    var a = leadership(Duration.ofMillis(300)); // live heartbeat renews ~every 100ms
    assertEquals(1L, a.tryAcquire("p5").orElseThrow().epoch());
    assertEquals(1L, a.current("p5").orElseThrow().epoch());
    // A live heartbeat keeps A's lease renewed, so a natural expiry-and-takeover races A's own
    // renew. Instead simulate the takeover atomically (exactly what a second acquirer writes:
    // new holder_id + bumped epoch). A's next heartbeat renew can no longer match its WHERE clause.
    simulateSteal("p5");
    awaitCurrentEmpty(a, "p5", 2000); // heartbeat detects the failed renew and drops the name
  }

  @Test
  void resignThenReacquireBumpsEpochNotResetToOne() throws Exception {
    var a = leadership(Duration.ofSeconds(30));
    assertEquals(1L, a.tryAcquire("p6").orElseThrow().epoch());
    a.resign("p6"); // must EXPIRE, never DELETE
    assertEquals(1L, dbRowCount("p6"), "resign must not delete the row (epoch monotonicity)");
    Thread.sleep(10);
    Optional<Lease> reacquired = a.tryAcquire("p6");
    assertTrue(reacquired.isPresent());
    assertEquals(
        2L,
        reacquired.get().epoch(),
        "resign EXPIRES not DELETEs — reacquire must bump to epoch 2, never reset to 1");
  }

  @Test
  void reAcquireWhileHoldingLiveLeaseReturnsSameEpoch() {
    var a = leadership(Duration.ofSeconds(30));
    assertEquals(1L, a.tryAcquire("p7").orElseThrow().epoch());
    // Own lease is live → ACQUIRE returns no row → the renew fallback confirms we still hold it.
    Optional<Lease> again = a.tryAcquire("p7");
    assertTrue(again.isPresent(), "re-acquire of a live lease we hold must succeed via renew");
    assertEquals(1L, again.get().epoch());
  }

  @Test
  void reacquireAfterStolenLeaseReturnsEmptyAndDrops() throws Exception {
    var a = leadership(Duration.ofSeconds(30)); // interval 10s: heartbeat will not fire in-test
    assertEquals(1L, a.tryAcquire("p8").orElseThrow().epoch());
    simulateSteal("p8"); // another replica now holds a live lease at epoch 2
    // ACQUIRE finds a live lease (no row); renew fallback (holder=A, epoch=1) fails → drop → empty.
    assertTrue(
        a.tryAcquire("p8").isEmpty(), "must not re-acquire a lease stolen by another holder");
    assertTrue(a.current("p8").isEmpty(), "the failed renew must drop the name from held");
  }

  @Test
  void currentIsEmptyForNeverAcquired() {
    var a = leadership(Duration.ofSeconds(30));
    assertTrue(a.current("never-touched").isEmpty());
  }

  @Test
  void tryAcquireAfterCloseReturnsEmpty() {
    var a = leadership(Duration.ofSeconds(30));
    a.close();
    assertTrue(a.tryAcquire("p9").isEmpty(), "a closed instance must not acquire");
  }

  @Test
  void sqlExceptionOnAcquireFailsClosed() {
    var faulting =
        new FaultInjectingDataSource(dataSource).failAfter(0); // every getConnection throws
    var a = new LeaseBasedLeadership(faulting, Duration.ofSeconds(1));
    managed.add(a);
    assertTrue(a.tryAcquire("p10").isEmpty(), "SQLException on acquire must fail closed (standby)");
    assertTrue(a.current("p10").isEmpty());
  }

  @Test
  void transientAcquireFailureDoesNotDropHeldLiveLease() throws Exception {
    // Repro: runners call tryAcquire on EVERY tick while already the healthy leader. A
    // transient SQLException (momentary pool exhaustion) historically ran held.remove(...)
    // unconditionally — locally discarding a lease whose DB row is still LIVE under this holder.
    // The tick was skipped, the heartbeat stopped renewing the name, and NO other replica could
    // acquire the live lease either: a guaranteed leaderless window of up to one full TTL (or a
    // whole cron period) per blip, ended by a forced epoch bump. The local drop is unnecessary for
    // safety — RENEW_SQL (holder_id AND epoch AND lease_until > now()) is the arbiter and the
    // Staleness bound fail-closes a persistent outage — so a transient acquire failure
    // must not revoke locally-held leadership the DB still attests.
    var faulting = new FaultInjectingDataSource(dataSource).failNth(2);
    var a = new LeaseBasedLeadership(faulting, Duration.ofSeconds(30)); // heartbeat idle in-test
    managed.add(a);
    assertEquals(1L, a.tryAcquire("p-blip").orElseThrow().epoch()); // attempt 1: healthy leader

    assertTrue(
        a.tryAcquire("p-blip").isEmpty(), // attempt 2: the blip — THIS call fails closed
        "the failing tryAcquire itself returns empty (its tick is skipped)");

    assertEquals(
        1L,
        a.current("p-blip").orElseThrow().epoch(),
        "a transient acquire failure must not revoke leadership the DB still attests");
    // The heartbeat's next renew still matches our live row — the lease was never stranded.
    try (var conn = dataSource.getConnection()) {
      assertTrue(
          a.renew(conn, "p-blip", 1L).isPresent(),
          "the next heartbeat renew must still match the kept lease's row");
    }
    // And the next tick resumes leadership at the SAME epoch — no leaderless TTL, no epoch bump.
    assertEquals(
        1L,
        a.tryAcquire("p-blip").orElseThrow().epoch(), // attempt 3: healthy again
        "the next tick must resume the held lease without an epoch bump");
    assertEquals(1L, dbEpoch("p-blip"), "no takeover/no bump happened at the DB either");
  }

  @Test
  void acquireFailureThenGenuineTakeover_keptEntryIsPrunedFailClosed() throws Exception {
    // Companion: keeping the held entry across a blip must NOT create zombie leadership
    // when the lease really was lost — e.g. the outage window let another replica take over
    // (epoch bumped server-side). The kept entry's next renew-confirm (old holder/epoch) matches
    // no row and prunes it: fail-closed arbitration stays with RENEW_SQL, exactly as designed.
    var faulting = new FaultInjectingDataSource(dataSource).failNth(2);
    var a = new LeaseBasedLeadership(faulting, Duration.ofSeconds(30));
    managed.add(a);
    assertEquals(1L, a.tryAcquire("p-blip-lost").orElseThrow().epoch()); // attempt 1
    assertTrue(a.tryAcquire("p-blip-lost").isEmpty()); // attempt 2: blip — entry kept
    simulateSteal("p-blip-lost"); // meanwhile another replica takes over (epoch 2, other holder)
    assertTrue(
        a.tryAcquire("p-blip-lost").isEmpty(), // attempt 3: renew-confirm matches no row
        "a genuinely lost lease must not be re-acquired");
    assertTrue(
        a.current("p-blip-lost").isEmpty(),
        "the failed renew-confirm must prune the kept entry (no zombie leadership)");
  }

  @Test
  void uncheckedAcquireFailureFailsClosedAndKeepsHeldLease() throws Exception {
    // Repro: runners call tryAcquire bare on every tick. The earlier catch hardened only
    // SQLException, but the failure class the heartbeat explicitly documents as expected
    // and retryable — a wrapped/routing/lazy DataSource throwing IllegalStateException from
    // getConnection() — passed straight through: ContinuousProjectionRunner.awaitLeadership let it
    // reach run()'s catch(Throwable) (state=ERROR, terminal death), and ScheduledProjectionRunner's
    // runLoop has no catch at all around its tryAcquire, so the projection thread died silently as
    // STOPPED. A transient infra blip became a permanent, restart-requiring, per-replica outage —
    // whereas the byte-equivalent SQLException in the same position was a skipped tick. Any
    // transient failure (checked or unchecked) must degrade to exactly the behavior:
    // fail THIS CALL closed, keep the held live lease, resume on the next tick with no epoch bump.
    var faulting =
        new FaultInjectingDataSource(dataSource)
            .failNthUnchecked(2, () -> new IllegalStateException("wrapped DataSource misroute"));
    var a = new LeaseBasedLeadership(faulting, Duration.ofSeconds(30)); // heartbeat idle in-test
    managed.add(a);
    assertEquals(1L, a.tryAcquire("p-rte").orElseThrow().epoch()); // attempt 1: healthy leader

    Optional<Lease> blipped = assertDoesNotThrow(() -> a.tryAcquire("p-rte")); // attempt 2: blip
    assertTrue(blipped.isEmpty(), "an unchecked acquire failure must skip the tick, never throw");

    assertEquals(
        1L,
        a.current("p-rte").orElseThrow().epoch(),
        "the held live lease must be kept across the unchecked blip");
    assertEquals(
        1L,
        a.tryAcquire("p-rte").orElseThrow().epoch(), // attempt 3: healthy again
        "the next tick must resume the held lease without an epoch bump");
    assertEquals(1L, dbEpoch("p-rte"), "no takeover/no bump happened at the DB either");
  }

  @Test
  void uncheckedAcquireFailureWithNothingHeldFailsClosedAndRecovers() {
    // The standby/first-acquire shape of the same blip — no lease held yet, the very
    // first getConnection throws unchecked. The call must return empty (standby, tick skipped)
    // and the next tick must acquire normally.
    var faulting =
        new FaultInjectingDataSource(dataSource)
            .failNthUnchecked(1, () -> new IllegalStateException("wrapped DataSource torn down"));
    var a = new LeaseBasedLeadership(faulting, Duration.ofSeconds(30));
    managed.add(a);
    Optional<Lease> blipped = assertDoesNotThrow(() -> a.tryAcquire("p-rte-first"));
    assertTrue(blipped.isEmpty(), "the failing tick is skipped (standby), never a throw");
    assertTrue(a.current("p-rte-first").isEmpty());
    assertEquals(
        1L,
        a.tryAcquire("p-rte-first").orElseThrow().epoch(),
        "the next tick must acquire normally once the blip clears");
  }

  @Test
  void errorOnAcquirePropagatesToCaller() {
    // Error policy (deliberate): an Error (OOME, StackOverflowError, LinkageError) on the
    // acquire path is NOT converted into a skipped tick — swallowing it would silently mask a fatal
    // JVM condition as "standby". It propagates to the calling runner, whose own Error discipline
    // terminates loudly with terminal-DOWN health. standDownTerminally
    // stays heartbeat-only: a caller-thread Error does not prove the heartbeat singleton is broken,
    // and closing the whole shared instance would strand every OTHER runner on this replica.
    var faulting =
        new FaultInjectingDataSource(dataSource)
            .failNthUnchecked(1, () -> new AssertionError("fatal acquire-path error"));
    var a = new LeaseBasedLeadership(faulting, Duration.ofSeconds(30));
    managed.add(a);
    assertThrows(AssertionError.class, () -> a.tryAcquire("p-acquire-error"));
  }

  @Test
  void resignSurvivesUncheckedDataSourceFailure() {
    // Repro: the runner finally blocks (ContinuousProjectionRunner.run,
    // ScheduledProjectionRunner.runLoop) and close() paths (DeadLetterRetryRunner,
    // SagaCompensationRetrySweeper) call resign() relying on its documented "no-throw, safe on a
    // dead connection" contract — but the catch covered only SQLException. A wrapped/routing
    // DataSource throwing IllegalStateException (typically present at EXACTLY the moment a runner
    // is dying from the same broken DataSource) made resign() throw inside those finallys,
    // truncating them: terminal-DOWN health marking skipped (health UP forever over a frozen read
    // model), the run-completion latch never counted (close() hangs its full join timeout), the
    // runner left not-restartable. resign must be genuinely no-throw for SQL and unchecked
    // failures alike: WARN, and let the lease expire naturally at TTL.
    var faulting =
        new FaultInjectingDataSource(dataSource)
            .failNthUnchecked(2, () -> new IllegalStateException("wrapped DataSource torn down"));
    var a = new LeaseBasedLeadership(faulting, Duration.ofSeconds(30));
    managed.add(a);
    assertEquals(1L, a.tryAcquire("p-resign-rte").orElseThrow().epoch()); // attempt 1

    assertDoesNotThrow(
        () -> a.resign("p-resign-rte"), // attempt 2: getConnection throws IllegalStateException
        "resign must honor its no-throw contract for unchecked failures too");
    assertTrue(
        a.current("p-resign-rte").isEmpty(),
        "the held entry is dropped locally even when the DB expire could not be written");
  }

  @Test
  void closeSurvivesUncheckedDataSourceFailure() {
    // Companion: close() resigns every held name; an unchecked failure inside one of
    // those resigns must not escape close() (lifecycle adapters call it during context shutdown).
    var faulting =
        new FaultInjectingDataSource(dataSource)
            .failNthUnchecked(2, () -> new IllegalStateException("wrapped DataSource torn down"));
    var a = new LeaseBasedLeadership(faulting, Duration.ofSeconds(30));
    managed.add(a);
    assertEquals(1L, a.tryAcquire("p-close-rte").orElseThrow().epoch()); // attempt 1
    assertDoesNotThrow(a::close, "close() must survive an unchecked resign failure");
    assertTrue(a.current("p-close-rte").isEmpty());
  }

  @Test
  void resignIsIdempotentAndSafeWhenNotHeld() {
    var a = leadership(Duration.ofSeconds(30));
    a.resign("never-held"); // no throw when nothing is held
    a.tryAcquire("p11");
    a.resign("p11");
    a.resign("p11"); // idempotent
    assertTrue(a.current("p11").isEmpty());
  }

  @Test
  void closeIsIdempotent() {
    var a = leadership(Duration.ofSeconds(30));
    a.tryAcquire("p12");
    a.close();
    a.close(); // second close is a no-op, never throws
    assertTrue(a.current("p12").isEmpty());
  }

  @Test
  void renewFailurePastTtl_currentFailsClosed() throws Exception {
    // A leader that loses DB connectivity keeps its held entry (the renew heartbeat
    // cannot prune it — it never even reaches a "renew matched no row"). Its current() must not
    // keep reporting leadership for the whole outage: once more than one TTL has elapsed since the
    // last successful renew, the DB lease has expired and a standby may have taken over, so
    // current() must fail closed.
    var clock = MutableClock.startingAt(Instant.parse("2026-07-19T00:00:00Z"));
    var ttl = Duration.ofSeconds(15);
    // failAfter(1): the initial acquire's getConnection succeeds; every subsequent heartbeat renew
    // getConnection throws — modeling a leader partitioned away from PostgreSQL.
    var faulting = new FaultInjectingDataSource(dataSource).failAfter(1);
    var a = new LeaseBasedLeadership(faulting, ttl, clock);
    managed.add(a);

    assertEquals(1L, a.tryAcquire("p-renew-fail").orElseThrow().epoch());
    // Immediately after acquire: fresh, still leader.
    assertEquals(1L, a.current("p-renew-fail").orElseThrow().epoch());

    // Time advances past the TTL with no successful renew (all renews fail).
    clock.advance(ttl.plusMillis(1));
    assertTrue(
        a.current("p-renew-fail").isEmpty(),
        "current() must fail closed once renews have failed for longer than the lease TTL");
  }

  @Test
  void heartbeatSurvivesUncheckedCycleFailureAndResumesRenewing() throws Exception {
    // Repro: renewLoop historically caught only InterruptedException (around sleep) and
    // renewHeldLeases only SQLException, so ANY unchecked throwable escaping a cycle — e.g. a
    // wrapped/routing DataSource whose getConnection() throws IllegalStateException — killed the
    // singleton heartbeat virtual thread for the remaining process lifetime: no renews, no
    // staleness prunes, leadership silently flapping at TTL cadence forever. The heartbeat must
    // treat an unchecked per-cycle failure like the SQLException it already tolerates: log it and
    // retry the next cycle.
    //
    // The clock is FROZEN so the staleness prune can never drop the entry: continued
    // getConnection attempts and DB renews are attributable only to heartbeat survival.
    var clock = MutableClock.startingAt(Instant.parse("2026-07-19T00:00:00Z"));
    var faulting =
        new FaultInjectingDataSource(dataSource)
            .failNthUnchecked(2, () -> new IllegalStateException("wrapped DataSource torn down"));
    var a = new LeaseBasedLeadership(faulting, Duration.ofMillis(300), clock); // heartbeat ~100ms
    managed.add(a);

    assertEquals(1L, a.tryAcquire("p-hb-rte").orElseThrow().epoch()); // getConnection attempt 1
    double leaseUntilAfterAcquire = dbLeaseUntilEpochSeconds("p-hb-rte");

    // Heartbeat cycle 1 = getConnection attempt 2 → IllegalStateException. A dead heartbeat never
    // calls getConnection again (attempts stuck at 2); a surviving one retries the next cycle.
    awaitTrue(
        () -> faulting.connectionAttempts() >= 3,
        "heartbeat must survive an unchecked per-cycle failure and attempt the next cycle");
    // ...and those later cycles genuinely renew: the DB lease keeps being extended.
    awaitTrue(
        () -> dbLeaseUntilEpochSeconds("p-hb-rte") > leaseUntilAfterAcquire,
        "surviving heartbeat cycles must keep renewing the lease at the database");
    assertEquals(
        1L,
        a.current("p-hb-rte").orElseThrow().epoch(),
        "leadership must hold steadily across the cycle failure — no flap, no epoch bump");
  }

  @Test
  void heartbeatUnrecoverableErrorStandsDownLoudlyAndPermanently() throws Exception {
    // Error policy: an Error (OOME, StackOverflowError, LinkageError) is NOT retryable —
    // mirroring the discipline of every other long-lived loop, the
    // heartbeat stops terminally but must stand down LOUDLY first: mark the instance closed
    // (tryAcquire refuses from now on — otherwise runners would keep re-acquiring leases the dead
    // heartbeat can never renew and flap at TTL cadence), clear `held` so current() reports
    // standby immediately, and best-effort EXPIRE the DB leases so standbys take over promptly.
    var clock = MutableClock.startingAt(Instant.parse("2026-07-19T00:00:00Z")); // frozen: the
    // staleness bound never trips, so an EMPTY current() is attributable only to the stand-down.
    var faulting =
        new FaultInjectingDataSource(dataSource)
            .failNthUnchecked(2, () -> new AssertionError("unrecoverable heartbeat error"));
    var a = new LeaseBasedLeadership(faulting, Duration.ofMillis(300), clock);
    managed.add(a);
    assertEquals(1L, a.tryAcquire("p-hb-err").orElseThrow().epoch());

    // The Error on heartbeat cycle 1 must stand the replica down, not leave it claiming (frozen
    // clock: forever-unstale) leadership over a lease nobody renews.
    awaitCurrentEmpty(a, "p-hb-err", 10_000);
    assertTrue(
        a.tryAcquire("p-hb-err").isEmpty(),
        "a terminally stood-down instance must never hand out leases again");

    // The lease is genuinely released (stand-down EXPIREs it): a healthy replica takes over with
    // the fencing-epoch bump instead of the name staying stranded.
    var b = leadership(Duration.ofSeconds(30));
    awaitTrue(
        () -> b.tryAcquire("p-hb-err").isPresent(),
        "a standby must be able to take over the stood-down lease");
    assertEquals(
        2L,
        b.tryAcquire("p-hb-err").orElseThrow().epoch(),
        "takeover of the stood-down lease bumps the fencing epoch");
  }

  @Test
  void resignedLeaseCannotBeResurrectedByRacingRenew() throws Exception {
    // resign() EXPIRES the lease (lease_until = now()). The shared heartbeat may hold an
    // in-flight renew for the name, captured just before resign removed it from the held map; that
    // renew must NOT resurrect the expired lease. RENEW_SQL's `AND lease_until > now()` makes a
    // renew extend only a still-live lease, so a renew racing/following a resign matches no row and
    // the lease stays resigned — a standby can take over immediately instead of waiting a full TTL.
    var a = leadership(Duration.ofSeconds(30)); // long ttl: heartbeat will not fire during the test
    assertEquals(1L, a.tryAcquire("p-resign-race").orElseThrow().epoch());

    a.resign("p-resign-race"); // EXPIRES lease_until = now()

    // The in-flight renew the heartbeat would have issued (same holder + epoch) must not resurrect.
    try (var conn = dataSource.getConnection()) {
      assertTrue(
          a.renew(conn, "p-resign-race", 1L).isEmpty(),
          "a renew must not resurrect a resigned (expired) lease");
    }

    // The resigned lease is genuinely free: a second instance takes it over (epoch bumps to 2).
    Thread.sleep(10);
    var b = leadership(Duration.ofSeconds(30));
    assertEquals(
        2L,
        b.tryAcquire("p-resign-race").orElseThrow().epoch(),
        "the resigned lease must be free for takeover, not resurrected by a racing renew");
  }

  @Test
  void renewQueuedBehindAResignCannotResurrectTheLease() throws Exception {
    // The order in which two statements take the row lock is not the order in which they started.
    // A renew that started before a resign, but reaches the row after the resign's EXPIRE took the
    // lock, re-checks its predicate against the expired row once the EXPIRE commits. The renew's
    // transaction time is older than the expiry, so a comparison against it would still see a
    // live lease and extend the resigned lease for a full TTL.
    var a = leadership(Duration.ofSeconds(30)); // long ttl: heartbeat will not fire during the test
    assertEquals(1L, a.tryAcquire("p-renew-behind-expire").orElseThrow().epoch());

    try (var renewConn = dataSource.getConnection();
        var expireConn = dataSource.getConnection()) {
      renewConn.setAutoCommit(false);
      int renewPid;
      try (var stmt = renewConn.createStatement();
          var rs = stmt.executeQuery("SELECT pg_backend_pid()")) { // the renew's time starts here
        rs.next();
        renewPid = rs.getInt(1);
      }
      Thread.sleep(20);

      expireConn.setAutoCommit(false);
      try (var ps =
          expireConn.prepareStatement(
              "UPDATE subscription_leases SET lease_until = now() WHERE consumer_name = ?")) {
        ps.setString(1, "p-renew-behind-expire");
        assertEquals(1, ps.executeUpdate()); // the resign's EXPIRE holds the row lock
      }

      var renewed =
          java.util.concurrent.CompletableFuture.supplyAsync(
              () -> {
                try {
                  return a.renew(renewConn, "p-renew-behind-expire", 1L);
                } catch (java.sql.SQLException e) {
                  throw new IllegalStateException(e);
                }
              });
      awaitTrue(() -> waitsOnALock(renewPid), "the renew must queue behind the EXPIRE's row lock");

      expireConn.commit();
      assertTrue(
          renewed.get(10, java.util.concurrent.TimeUnit.SECONDS).isEmpty(),
          "a renew queued behind a resign must not extend the resigned lease");
      renewConn.commit();
    }

    var b = leadership(Duration.ofSeconds(30));
    assertEquals(
        2L,
        b.tryAcquire("p-renew-behind-expire").orElseThrow().epoch(),
        "the resigned lease must be free for takeover at once");
  }

  private boolean waitsOnALock(int pid) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT count(*) FROM pg_stat_activity WHERE pid = ? AND wait_event_type = 'Lock'")) {
      ps.setInt(1, pid);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1) == 1;
      }
    }
  }

  @Test
  void rejectsNullDataSource() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new LeaseBasedLeadership(null, Duration.ofSeconds(1)));
  }

  @Test
  void rejectsNonPositiveTtl() {
    assertThrows(
        IllegalArgumentException.class, () -> new LeaseBasedLeadership(dataSource, Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class,
        () -> new LeaseBasedLeadership(dataSource, Duration.ofSeconds(-1)));
    assertThrows(IllegalArgumentException.class, () -> new LeaseBasedLeadership(dataSource, null));
  }

  /**
   * The same truncation class as the sub-millisecond lock timeout in {@code PgAdvisoryLocker}.
   * {@code PT0.0005S} is strictly positive, so {@code rejectsNonPositiveTtl}'s check passed it, and
   * {@code toMillis()} then made {@code ttlMillis = 0}: every lease was written already expired, so
   * the {@code lease_until < now()} takeover guard fired for every replica on every poll and
   * leadership churned continuously. Epoch fencing bounds that to churn rather than corruption,
   * which is why it is a minor — but it is the same rule, so it is rejected rather than silently
   * rounded up.
   */
  @Test
  void rejectsSubMillisecondTtl() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> new LeaseBasedLeadership(dataSource, Duration.ofNanos(500_000)));
    assertTrue(
        ex.getMessage().contains("at least 1ms"),
        "message must name the millisecond floor, got: " + ex.getMessage());
  }

  /** The floor itself stays a valid TTL — the rule rejects the band below it, not the band. */
  @Test
  void acceptsOneMillisecondTtl() {
    assertDoesNotThrow(() -> leadership(Duration.ofMillis(1)));
  }
}
