package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/**
 * Deterministic "a concurrent writer touches a candidate row inside a retention DELETE's window"
 * harness for the batched purge loops.
 *
 * <p>The writer's statement runs in a held transaction, so its row lock is taken before the sweep
 * starts. The sweep then runs on its own thread, and the harness waits until PostgreSQL reports a
 * backend blocked on the writer's lock — an observed server-side condition, not a sleep — before
 * committing the writer. The sweep's DELETE therefore always meets the concurrently committed
 * change through READ COMMITTED's EvalPlanQual re-check, which is the window in which a chunk comes
 * back shorter than its LIMIT although older rows remain.
 */
final class RowLockRaces {

  private RowLockRaces() {}

  /**
   * Runs {@code writerSql} (binding {@code params} as strings, expecting exactly one affected row)
   * in an open transaction, starts {@code sweep}, waits until a backend blocks on the writer's row
   * lock, commits the writer and returns the sweep's result.
   */
  static <T> T sweepWhileAWriterHoldsARow(
      DataSource dataSource, String writerSql, Callable<T> sweep, String... params)
      throws Exception {
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try (Connection writer = dataSource.getConnection()) {
      writer.setAutoCommit(false);
      int writerPid = backendPid(writer);
      try (PreparedStatement ps = writer.prepareStatement(writerSql)) {
        for (int i = 0; i < params.length; i++) {
          ps.setString(i + 1, params[i]);
        }
        assertEquals(1, ps.executeUpdate(), "the concurrent writer must touch exactly one row");
      }

      Future<T> result = pool.submit(sweep);
      awaitBlockedBy(dataSource, writerPid, 60);
      writer.commit();
      return result.get(60, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }
  }

  private static int backendPid(Connection conn) throws Exception {
    try (PreparedStatement ps = conn.prepareStatement("SELECT pg_backend_pid()");
        ResultSet rs = ps.executeQuery()) {
      rs.next();
      return rs.getInt(1);
    }
  }

  /**
   * Blocks until PostgreSQL reports some backend waiting on a lock held by {@code holderPid}. The
   * poll interval only bounds how quickly the wait is noticed; the exit condition is the lock wait
   * itself. Fails if the block never materializes within {@code timeoutSeconds}.
   */
  private static void awaitBlockedBy(DataSource dataSource, int holderPid, int timeoutSeconds)
      throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT count(*) FROM pg_stat_activity a "
                    + "WHERE a.pid <> pg_backend_pid() AND ? = ANY(pg_blocking_pids(a.pid))")) {
      ps.setInt(1, holderPid);
      while (System.nanoTime() < deadline) {
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next() && rs.getLong(1) > 0) {
            return;
          }
        }
        TimeUnit.MILLISECONDS.sleep(20); // poll interval only — the exit condition is the lock wait
      }
    }
    throw new AssertionError(
        "no backend blocked on the row lock held by pid "
            + holderPid
            + " within "
            + timeoutSeconds
            + "s — the sweep's DELETE did not reach the row lock");
  }
}
