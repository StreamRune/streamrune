package org.streamrune.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.subscription.SubscriptionLeadership;

/**
 * PostgreSQL {@link SubscriptionLeadership} backed by a persisted, <b>DB-clocked</b> lease with a
 * monotonic fencing {@code epoch}. Every operation is a single atomic statement over the {@code
 * subscription_leases} table (event-store baseline), so leadership is correct under any connection
 * pooling — including PgBouncer transaction mode, where a session-scoped {@code pg_advisory_lock}
 * would be unsafe. No connection is held across calls.
 *
 * <h2>Acquire</h2>
 *
 * A single upsert decides leadership using the database clock, never the JVM clock:
 *
 * <pre>{@code
 * INSERT ... VALUES (name, holderId, 1, now() + ttl)
 * ON CONFLICT (consumer_name) DO UPDATE
 *   SET holder_id = EXCLUDED.holder_id, epoch = subscription_leases.epoch + 1, lease_until = EXCLUDED.lease_until
 *   WHERE subscription_leases.lease_until < now()
 * RETURNING epoch
 * }</pre>
 *
 * A fresh insert yields epoch {@code 1}; taking over an <em>expired</em> lease bumps the epoch by
 * one (the fencing token); a live lease held by another replica matches no row (the {@code WHERE}
 * guard is false) and returns nothing. On no row we might already hold a <em>live</em> lease
 * ourselves (a re-acquire), so we confirm with a {@code RENEW}; if that also returns nothing the
 * lease is gone and we drop it — fail-closed standby.
 *
 * <h2>Renew heartbeat</h2>
 *
 * A virtual thread renews every held lease on an interval of {@code ttl/3} (min 1ms). {@code
 * current(name)} is a purely local read of the held map — no DB probe — because the heartbeat
 * maintains its truth: a lease stolen by a newer leader makes the renew's {@code WHERE holder_id =
 * ? AND epoch = ?} match no row, so the name is dropped and {@code current} reports standby. The
 * brief stale-current window (between a steal and the next heartbeat) is covered downstream by the
 * epoch write-fence in {@code executeAtomically}.
 *
 * <p><b>Heartbeat resilience.</b> The heartbeat is a process-lifetime singleton: if it died, every
 * held lease would expire unrenewed and the replica would silently flap
 * leader→standby→re-acquire(epoch+1) at TTL cadence forever. A per-cycle failure of any kind —
 * including unchecked throwables from a wrapped or torn-down {@code DataSource} — is therefore
 * logged and retried next cycle. An unrecoverable {@link Error} is not retryable: the heartbeat
 * stops terminally but stands down loudly first — the instance closes ({@code tryAcquire} refuses
 * from then on), {@code held} is cleared so {@code current} reports standby immediately, and each
 * lease is best-effort expired so standbys take over promptly.
 *
 * <p><b>Renew-failure fail-closed.</b> A renew that observably fails (matches no row) prunes the
 * name immediately. But a renew that cannot reach the database at all — a partial partition where
 * this replica loses PostgreSQL while a peer keeps it — throws before it can observe the steal, so
 * the held entry would otherwise be retained for the entire outage while a standby takes over:
 * {@code current} would stay fail-<em>open</em> indefinitely. To bound this, each held name tracks
 * the instant of its last <em>successful</em> renew (seeded at acquire). Once more than one lease
 * TTL of local time has elapsed since that instant, the DB lease has certainly expired and a
 * standby may have taken over, so {@code current} returns empty (fail-closed) and the heartbeat
 * drops the name — restoring the documented invariant that {@code current} is stale for at most one
 * heartbeat interval past lease expiry.
 *
 * <h2>Resign EXPIRES, never DELETEs</h2>
 *
 * {@code resign} sets {@code lease_until = now()} for our own holder id rather than deleting the
 * row. Deleting would reset the next acquire to epoch {@code 1}, which could fall <em>below</em> an
 * epoch already stamped into {@code projection_offset} and wrongly fence out a legitimate new
 * leader. Keeping the row preserves epoch monotonicity across resign → reacquire.
 *
 * <p><b>Resign/renew race.</b> The shared heartbeat may hold an in-flight renew for a name captured
 * just before {@code resign} removed it. {@code RENEW_SQL} therefore requires {@code lease_until >
 * clock_timestamp()}: a renew can extend only a still-live lease, so a renew that races (or
 * follows) a resign matches no row and cannot resurrect the just-expired lease. The comparison uses
 * the time the row is checked, not the statement's start time ({@code now()}): a renew that started
 * before the resign but reached the row after the resign's EXPIRE took the row lock re-checks the
 * expired row once the EXPIRE commits, and its own older {@code now()} would still be before the
 * expiry. Without this the DB would show a live lease held by a resigned replica until the
 * resurrected lease's TTL, leaving the projection leaderless despite a clean stop.
 *
 * <h2>Failure handling</h2>
 *
 * A {@link SQLException} <em>or unchecked {@link RuntimeException}</em> on acquire (a
 * wrapped/routing/lazy DataSource throwing {@code IllegalStateException} is the same transient
 * class the heartbeat tolerates) is logged at WARN and returns empty for that call — a leadership
 * blip must degrade a runner to a skipped tick, never crash its loop; the runners call {@code
 * tryAcquire} bare on their tick paths, so a throw here is a runner death. It does <b>not</b> drop
 * a lease this instance already holds: the DB row is typically still live under this holder, and
 * discarding the entry would stop our own renewals while no other replica can acquire the live
 * lease — stranding the name leaderless until {@code lease_until}. The held map is not the safety
 * arbiter; {@code RENEW_SQL}'s {@code holder_id AND epoch AND lease_until > clock_timestamp()}
 * guard and the staleness bound are: a genuinely lost lease is pruned by the next renew, and a
 * persistent outage fail-closes {@code current()} after one TTL. An {@link Error} is never
 * swallowed: it propagates to the calling runner's own loud-termination discipline (see the note in
 * {@code tryAcquire}). {@code resign} is genuinely no-throw for SQL and unchecked failures alike —
 * the runner {@code finally} blocks that call it must always complete their shutdown bookkeeping;
 * when the expire write cannot reach the database the lease simply expires at TTL. {@link #close()}
 * is idempotent, interrupts the heartbeat, and resigns every held name.
 */
public final class LeaseBasedLeadership implements SubscriptionLeadership {

  private static final Logger LOG = LoggerFactory.getLogger(LeaseBasedLeadership.class);

  private static final String ACQUIRE_SQL =
      """
      INSERT INTO subscription_leases (consumer_name, holder_id, epoch, lease_until)
      VALUES (?, ?, 1, now() + (? * interval '1 millisecond'))
      ON CONFLICT (consumer_name) DO UPDATE
        SET holder_id = EXCLUDED.holder_id,
            epoch = subscription_leases.epoch + 1,
            lease_until = EXCLUDED.lease_until
        WHERE subscription_leases.lease_until < now()
      RETURNING epoch
      """;

  private static final String RENEW_SQL =
      """
      UPDATE subscription_leases
         SET lease_until = now() + (? * interval '1 millisecond')
       WHERE consumer_name = ? AND holder_id = ? AND epoch = ? AND lease_until > clock_timestamp()
      RETURNING epoch
      """;

  private static final String EXPIRE_SQL =
      """
      UPDATE subscription_leases
         SET lease_until = now()
       WHERE consumer_name = ? AND holder_id = ?
      """;

  private final DataSource dataSource;
  private final long ttlMillis;
  private final Clock clock;
  private final String holderId = UUID.randomUUID().toString();
  private final ConcurrentHashMap<String, HeldLease> held = new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final Thread heartbeat;

  /**
   * A locally-held lease: its fencing {@code epoch} plus the instant of the last renew that
   * observably succeeded (seeded at acquire). The instant drives the fail-closed check — see {@link
   * #isStale(HeldLease)}.
   */
  private record HeldLease(long epoch, Instant lastSuccessfulRenew) {}

  /**
   * @param dataSource pooled data source used for every (stateless, single-statement) operation
   * @param leaseTtl how long a granted lease stays live before it may be taken over; the renew
   *     heartbeat runs at {@code leaseTtl/3}. Must be at least one millisecond — see the Note on
   *     the delegated constructor.
   * @throws IllegalArgumentException if {@code dataSource} is null, or {@code leaseTtl} is not a
   *     strictly positive duration of at least one millisecond
   */
  public LeaseBasedLeadership(DataSource dataSource, Duration leaseTtl) {
    this(dataSource, leaseTtl, Clock.systemUTC());
  }

  /**
   * Test seam: same as {@link #LeaseBasedLeadership(DataSource, Duration)} but with an injectable
   * clock so the last-successful-renew staleness window can be driven deterministically.
   *
   * <p>The same truncation class as the sub-millisecond lock timeout in {@code PgAdvisoryLocker},
   * one module over. {@code PT0.0005S} is strictly positive, so the check below passed it, and
   * {@code toMillis()} then made {@code ttlMillis = 0}: every lease written was already expired at
   * the instant it was written, so the {@code lease_until < now()} takeover guard fired for every
   * replica on every poll and leadership churned continuously. Epoch fencing bounds that to churn
   * rather than corruption, which is why it is a minor rather than a repeat of that bug — but the
   * millisecond floor is the same rule, so it is stated the same way rather than silently rounded
   * up to something the operator did not write.
   */
  LeaseBasedLeadership(DataSource dataSource, Duration leaseTtl, Clock clock) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource must not be null");
    }
    if (leaseTtl == null || leaseTtl.isZero() || leaseTtl.isNegative()) {
      throw new IllegalArgumentException("leaseTtl must be a positive duration");
    }
    if (leaseTtl.toMillis() < 1L) {
      throw new IllegalArgumentException(
          "leaseTtl must be at least 1ms — a shorter duration truncates to 0 and writes a lease"
              + " that is already expired, so every replica takes over on every poll; got: "
              + leaseTtl);
    }
    this.dataSource = dataSource;
    this.ttlMillis = leaseTtl.toMillis();
    this.clock = Objects.requireNonNull(clock, "clock must not be null");
    long intervalMillis = Math.max(1L, ttlMillis / 3);
    this.heartbeat =
        Thread.ofVirtual()
            .name("streamrune-lease-heartbeat-" + holderId)
            .start(() -> renewLoop(intervalMillis));
  }

  @Override
  public Optional<Lease> tryAcquire(String consumerName) {
    if (closed.get()) {
      return Optional.empty();
    }
    try (Connection conn = dataSource.getConnection()) {
      Optional<Long> acquired =
          PostgresTransactions.commitIfManual(conn, "lease acquire", c -> acquire(c, consumerName));
      if (acquired.isPresent()) {
        long epoch = acquired.get();
        held.put(consumerName, new HeldLease(epoch, clock.instant()));
        return Optional.of(new Lease(epoch));
      }
      // No row: the ON CONFLICT guard (lease_until < now()) did not fire, so a live lease exists.
      // It may be OURS (a re-acquire of a lease we already hold) — confirm with a renew.
      HeldLease own = held.get(consumerName);
      if (own != null) {
        Optional<Long> renewed =
            PostgresTransactions.commitIfManual(
                conn, "lease renew", c -> renew(c, consumerName, own.epoch()));
        if (renewed.isPresent()) {
          // Successful renew refreshes the last-successful-renew instant.
          held.put(consumerName, new HeldLease(renewed.get(), clock.instant()));
          return Optional.of(new Lease(renewed.get()));
        }
        // Renew matched no row: another holder took over. Drop it and stand by (fail-closed).
        held.remove(consumerName, own);
      }
      return Optional.empty();
    } catch (SQLException | RuntimeException e) {
      // Fail THIS CALL closed (empty — the caller skips its tick), but do NOT drop a
      // lease we already hold. Runners call tryAcquire on every tick while healthy leader; on a
      // transient blip (pool-exhaustion timeout, connection drop) the held lease's DB row is
      // typically still LIVE under our holder_id and still being renewed by the heartbeat.
      // Dropping the local entry would stop our renewals for the name while NO other replica can
      // acquire the live lease — a guaranteed leaderless window of up to one full TTL (a whole
      // cron period for scheduled projections), ended by a needless epoch bump. Keeping it is
      // safe because this map is not the safety arbiter — RENEW_SQL is (holder_id AND epoch AND
      // lease_until > clock_timestamp()): if this acquire actually executed server-side or
      // another replica took over during the outage, the next renew matches no row and prunes the
      // entry, and the staleness bound fail-closes current() if the outage outlasts one
      // TTL.
      //
      // RuntimeException gets the SAME degradation, not just SQLException. The failure
      // class the heartbeat already documents as expected and retryable — a
      // wrapped/routing/lazy DataSource throwing IllegalStateException from getConnection() —
      // previously escaped here into the runners, which call tryAcquire BARE on their tick paths:
      // ContinuousProjectionRunner died terminally (state=ERROR via its catch(Throwable)) and
      // ScheduledProjectionRunner's loop thread died silently as STOPPED — a transient blip became
      // a permanent, per-replica, restart-requiring outage. An Error is deliberately NOT caught:
      // swallowing an OOME/StackOverflowError as "standby" would mask a fatal JVM condition, and
      // the runners' own Error discipline terminates loudly with
      // terminal-DOWN health. standDownTerminally stays heartbeat-only — a caller-thread Error
      // does not prove the heartbeat singleton is broken, and closing the shared instance here
      // would strand every other runner on this replica.
      LOG.warn("tryAcquire('{}') failed; skipping this tick", consumerName, e);
      return Optional.empty();
    }
  }

  @Override
  public Optional<Lease> current(String consumerName) {
    HeldLease lease = held.get(consumerName);
    if (lease == null) {
      return Optional.empty();
    }
    // Fail closed if no renew has succeeded within the last TTL. The renew heartbeat
    // cannot prune a lease whose renews throw (DB unreachable), so without this bound current()
    // would keep reporting leadership for the whole outage even after a standby has taken over.
    if (isStale(lease)) {
      return Optional.empty();
    }
    return Optional.of(new Lease(lease.epoch()));
  }

  @Override
  public void resign(String consumerName) {
    held.remove(consumerName);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(EXPIRE_SQL)) {
      ps.setString(1, consumerName);
      ps.setString(2, holderId);
      PostgresTransactions.commitIfManual(conn, "lease expire", c -> ps.executeUpdate());
    } catch (SQLException | RuntimeException e) {
      // Genuinely no-throw for SQL AND unchecked failures. The runner finally blocks
      // (ContinuousProjectionRunner.run, ScheduledProjectionRunner.runLoop) and close() paths
      // (DeadLetterRetryRunner, SagaCompensationRetrySweeper, our own close()) call resign relying
      // on the documented "no-throw, safe on a dead connection" contract — and a wrapped/routing
      // DataSource throwing IllegalStateException is MOST likely at exactly the moment a runner is
      // dying from that same broken DataSource. A throw here truncated those finallys: terminal-
      // DOWN health marking skipped (health UP over a frozen read model), the run-completion latch
      // never counted (close() hung its full join timeout), restart guards never reset. The local
      // held entry is already removed above (current() reports standby immediately); the DB row
      // simply expires naturally at lease_until. Errors still propagate — the hardened runner
      // finallys complete their bookkeeping in a nested finally before an Error unwinds.
      LOG.warn("resign('{}') failed; lease will expire naturally at TTL", consumerName, e);
    }
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    heartbeat.interrupt();
    for (String consumerName : List.copyOf(held.keySet())) {
      resign(consumerName);
    }
  }

  private void renewLoop(long intervalMillis) {
    while (!closed.get()) {
      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException _) {
        Thread.currentThread().interrupt();
        return;
      }
      // This is the singleton, process-lifetime thread that renews AND staleness-prunes
      // every held lease. If it dies, leadership silently flaps at TTL cadence forever: the lease
      // expires unrenewed, the runner re-acquires it at epoch+1, nothing renews it, it expires
      // again — with no "standing down" log ever emitted. renewHeldLeases handles SQLException
      // itself, but anything unchecked (a wrapped/routing DataSource throwing IllegalStateException
      // from getConnection(), a proxy after partial context teardown) must be treated as the same
      // per-cycle failure: log and retry next cycle. An Error (OOME, StackOverflowError,
      // LinkageError) is NOT retryable — retrying would spin — so the heartbeat stops terminally,
      // but stands down LOUDLY first instead of leaving a silently flapping replica. This mirrors
      // the catch-Throwable discipline of every other long-lived loop (PollingEventSubscription,
      // ScheduledProjectionRunner, ContinuousProjectionRunner); like them
      // we do not rethrow — stand-down-and-report IS the handling.
      try {
        renewHeldLeases();
      } catch (Error e) {
        standDownTerminally(e);
        return;
      } catch (Throwable t) {
        LOG.error("lease heartbeat cycle failed; retrying next cycle", t);
      }
    }
  }

  /**
   * Terminal stand-down after an unrecoverable heartbeat {@link Error}. Without its heartbeat this
   * instance must never lead again — any lease it handed out would expire unrenewed after one TTL
   * and the runner would re-acquire it at epoch+1, silently flapping forever. So: mark the instance
   * closed ({@code tryAcquire} refuses from now on), clear {@code held} so {@link #current} reports
   * standby immediately, and best-effort EXPIRE each lease at the database so standbys take over
   * promptly instead of waiting out the TTL.
   */
  private void standDownTerminally(Error fatal) {
    closed.set(true);
    List<String> names = List.copyOf(held.keySet());
    held.clear();
    LOG.error(
        "lease heartbeat terminated on an unrecoverable error — standing down from leases {}"
            + " permanently; this replica will not lead again until restarted",
        names,
        fatal);
    for (String name : names) {
      try {
        resign(name);
      } catch (RuntimeException e) {
        LOG.warn("could not expire lease '{}' during stand-down; it will expire at TTL", name, e);
      }
    }
  }

  private void renewHeldLeases() {
    if (held.isEmpty()) {
      return;
    }
    // Stand down from any lease whose last successful renew is older than the TTL, BEFORE
    // attempting more renews. This covers the case where renews (or getConnection) throw and so
    // never observe a steal — without it the held entry would survive the whole DB outage.
    pruneStaleLeases();
    if (held.isEmpty()) {
      return;
    }
    try (Connection conn = dataSource.getConnection()) {
      for (var entry : held.entrySet()) {
        String consumerName = entry.getKey();
        HeldLease current = entry.getValue();
        try {
          if (PostgresTransactions.commitIfManual(
                  conn, "lease renew", c -> renew(c, consumerName, current.epoch()))
              .isEmpty()) {
            // Our lease was stolen or expired-and-taken → drop so current() reports standby.
            held.remove(consumerName, current);
            LOG.info("lease '{}' lost (renew matched no row); standing down", consumerName);
          } else {
            // Successful renew refreshes the last-successful-renew instant.
            held.replace(consumerName, current, new HeldLease(current.epoch(), clock.instant()));
          }
        } catch (SQLException e) {
          LOG.warn("renew('{}') failed; retrying next heartbeat", consumerName, e);
        }
      }
    } catch (SQLException e) {
      LOG.warn("lease heartbeat could not obtain a connection; retrying next cycle", e);
    }
  }

  /**
   * Drops every held name whose last successful renew is older than one lease TTL: the DB lease has
   * certainly expired and a standby may have taken over, so retaining it locally would make {@code
   * current} fail-open.
   */
  private void pruneStaleLeases() {
    for (var entry : held.entrySet()) {
      HeldLease lease = entry.getValue();
      if (isStale(lease) && held.remove(entry.getKey(), lease)) {
        LOG.info(
            "lease '{}' not renewed within TTL (DB unreachable); standing down (fail-closed)",
            entry.getKey());
      }
    }
  }

  /**
   * True once more than one lease TTL of local time has elapsed since the last successful renew.
   */
  private boolean isStale(HeldLease lease) {
    return clock.instant().isAfter(lease.lastSuccessfulRenew().plusMillis(ttlMillis));
  }

  private Optional<Long> acquire(Connection conn, String consumerName) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(ACQUIRE_SQL)) {
      ps.setString(1, consumerName);
      ps.setString(2, holderId);
      ps.setLong(3, ttlMillis);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rs.getLong(1)) : Optional.empty();
      }
    }
  }

  Optional<Long> renew(Connection conn, String consumerName, long epoch) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(RENEW_SQL)) {
      ps.setLong(1, ttlMillis);
      ps.setString(2, consumerName);
      ps.setString(3, holderId);
      ps.setLong(4, epoch);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rs.getLong(1)) : Optional.empty();
      }
    }
  }
}
