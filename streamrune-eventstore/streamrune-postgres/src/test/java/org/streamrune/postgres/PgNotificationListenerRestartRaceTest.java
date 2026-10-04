package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

/**
 * Regression: {@link PgNotificationListener#close()} must JOIN the listener thread before it
 * returns, so a {@code close()}/{@code start()} restart cannot leave the old listener thread
 * running concurrently with the new one (a leaked duplicate listener) — or, on the old thread's
 * give-up path, silently kill the restarted listener by flipping the shared {@code running} flag
 * false.
 *
 * <p>Uses mocked JDBC (no Testcontainers): the mocked {@link PGConnection#getNotifications(int)}
 * parks the listener thread until {@code close()} interrupts it, then lingers so the thread is
 * provably still alive when a NON-joining {@code close()} returns.
 */
class PgNotificationListenerRestartRaceTest {

  @Test
  void closeJoinsListenerThreadBeforeReturning() throws Exception {
    var listenerThread = new AtomicReference<Thread>();
    var entered = new CountDownLatch(1);

    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    Statement stmt = mock(Statement.class);
    PGConnection pgConn = mock(PGConnection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.createStatement()).thenReturn(stmt);
    when(conn.unwrap(PGConnection.class)).thenReturn(pgConn);
    when(pgConn.getNotifications(anyInt()))
        .thenAnswer(
            invocation -> {
              listenerThread.set(Thread.currentThread());
              entered.countDown();
              try {
                Thread.sleep(60_000);
              } catch (InterruptedException _) {
                // Slow shutdown: stay alive well past a non-joining close()'s return.
                try {
                  Thread.sleep(500);
                } catch (InterruptedException _) {
                  // no second interrupt expected
                }
                Thread.currentThread().interrupt();
              }
              return new PGNotification[0];
            });

    var listener = new PgNotificationListener(ds, () -> {}, "streamrune_events");
    listener.start();
    assertTrue(
        entered.await(5, TimeUnit.SECONDS), "listener thread should have entered getNotifications");

    listener.close();

    assertFalse(
        listenerThread.get().isAlive(), "close() must join the listener thread before returning");
  }
}
