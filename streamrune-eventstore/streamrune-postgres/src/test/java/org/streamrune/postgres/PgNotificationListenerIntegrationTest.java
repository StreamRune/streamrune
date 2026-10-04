package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Testcontainers-backed tests for {@link PgNotificationListener} that exercise the live listen
 * loop, notification dispatch, reconnect-with-backoff, the default never-give-up reconnect, and the
 * finite-cap giveup — paths the pure-unit {@link PgNotificationListenerTest} cannot reach.
 * Reconnect/giveup are driven deterministically with {@link FaultInjectingDataSource} rather than
 * relying on flaky timing.
 */
@Testcontainers
class PgNotificationListenerIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_listen_test");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void init() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
  }

  /** Issue a NOTIFY on the channel from an independent connection. */
  private static void notifyChannel(String channel) throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("NOTIFY " + channel);
    }
  }

  /** Wait until {@code latch} reaches zero, NOTIFYing every 200ms so a started LISTEN sees one. */
  private static boolean awaitNotification(CountDownLatch latch, String channel, long totalMs)
      throws Exception {
    long deadline = System.nanoTime() + totalMs * 1_000_000L;
    while (System.nanoTime() < deadline) {
      notifyChannel(channel);
      if (latch.await(200, TimeUnit.MILLISECONDS)) {
        return true;
      }
    }
    return false;
  }

  @Test
  void notificationInvokesCallback() throws Exception {
    String channel = "streamrune_events";
    var fired = new CountDownLatch(1);
    var listener = new PgNotificationListener(dataSource, fired::countDown, channel);
    try (listener) {
      listener.start();
      assertTrue(listener.isRunning());
      // Repeated NOTIFY covers the race between LISTEN registration and the first NOTIFY.
      assertTrue(
          awaitNotification(fired, channel, 10_000),
          "callback should fire when a NOTIFY arrives on the channel");
    }
    assertFalse(listener.isRunning(), "close() must stop the listener");
  }

  @Test
  void listenerReconnectsAfterConnectionDrop() throws Exception {
    String channel = "streamrune_events";
    // First getConnection() succeeds (initial LISTEN), the next two fail, then it recovers.
    // The failures here happen on reconnect attempts, exercising the backoff branch without
    // reaching MAX_RETRY_ATTEMPTS.
    var faultSource = new FaultInjectingDataSource(dataSource).failAfter(3);
    var hits = new AtomicInteger(0);
    var firstFired = new CountDownLatch(1);
    var afterReconnectFired = new CountDownLatch(2);
    Runnable callback =
        () -> {
          hits.incrementAndGet();
          firstFired.countDown();
          afterReconnectFired.countDown();
        };
    var listener = new PgNotificationListener(faultSource, callback, channel);
    try (listener) {
      listener.start();

      // 1. Healthy: first connection delivers a notification.
      assertTrue(awaitNotification(firstFired, channel, 10_000), "initial NOTIFY not delivered");

      // 2. Drop the listener's backend so getNotifications throws and it must reconnect. The next
      // getConnection() attempts (#2, #3) fail per failAfter(3); attempt #4 succeeds and
      // re-LISTENs.
      terminateListenerBackend(channel);

      // 3. After reconnect a fresh NOTIFY is delivered again (callback fires a second time).
      assertTrue(
          awaitNotification(afterReconnectFired, channel, 30_000),
          "NOTIFY after reconnect not delivered — reconnect path did not recover");
      assertTrue(
          listener.isRunning(), "listener should still be running after a successful reconnect");
      // More than one getConnection proves a reconnect actually occurred.
      assertTrue(
          faultSource.connectionAttempts() >= 2,
          "expected a reconnect (>=2 getConnection calls), saw "
              + faultSource.connectionAttempts());
    }
  }

  @Test
  void defaultListenerNeverGivesUpOnReconnect() throws Exception {
    String channel = "streamrune_events";
    // First getConnection() succeeds (LISTEN established); every subsequent attempt fails forever.
    var faultSource = new FaultInjectingDataSource(dataSource).failAfter(1);
    // Default (UNBOUNDED_RECONNECTS): the listener must NOT permanently give up. Fast backoff keeps
    // the test quick.
    var listener =
        PgNotificationListener.builder()
            .dataSource(faultSource)
            .onNotification(() -> {})
            .channel(channel)
            .backoff(10, 40)
            .build();
    listener.start();
    assertTrue(listener.isRunning());

    // Force the live connection to break so the loop re-enters getConnection(), which now always
    // fails. Before FIX 5 the listener gave up after 5 attempts; now it must keep retrying.
    terminateListenerBackend(channel);

    // Wait well past what 5 fast reconnect attempts would take; the listener must still be running.
    Thread.sleep(2_000);
    assertTrue(
        listener.isRunning(),
        "default listener must keep reconnecting forever, not give up after a fixed cap");
    listener.close();
  }

  @Test
  void listenerStopsAfterFiniteMaxReconnectAttempts() throws Exception {
    String channel = "streamrune_events";
    // First getConnection() succeeds (LISTEN established); every subsequent attempt fails forever.
    var faultSource = new FaultInjectingDataSource(dataSource).failAfter(1);
    // Finite cap: the listener stops itself after exhausting the configured reconnect budget, so a
    // fail-fast deployment can observe the dead push path via isRunning().
    var listener =
        PgNotificationListener.builder()
            .dataSource(faultSource)
            .onNotification(() -> {})
            .channel(channel)
            .maxReconnectAttempts(3)
            .backoff(10, 40)
            .build();
    listener.start();
    assertTrue(listener.isRunning());

    terminateListenerBackend(channel);

    long deadline = System.nanoTime() + 30_000L * 1_000_000L;
    while (listener.isRunning() && System.nanoTime() < deadline) {
      Thread.sleep(200);
    }
    assertFalse(
        listener.isRunning(),
        "listener should stop itself after exhausting the finite maxReconnectAttempts");
    listener.close();
  }

  /**
   * Terminates the backend(s) that are LISTENing on {@code channel}, simulating an abrupt
   * connection drop. The listener's {@code getNotifications} call then throws and triggers the
   * reconnect path.
   */
  private static void terminateListenerBackend(String channel) throws Exception {
    // Retry briefly: the LISTEN backend may not be registered in pg_stat_activity the instant we
    // ask (LISTEN runs shortly after the loop gets its connection).
    long deadline = System.nanoTime() + 10_000L * 1_000_000L;
    while (System.nanoTime() < deadline) {
      int terminated;
      try (var conn = dataSource.getConnection();
          var ps =
              conn.prepareStatement(
                  "SELECT count(pg_terminate_backend(pid))::int AS n FROM pg_stat_activity "
                      + "WHERE query ILIKE ? AND pid <> pg_backend_pid()")) {
        ps.setString(1, "LISTEN%" + channel + "%");
        try (var rs = ps.executeQuery()) {
          rs.next();
          terminated = rs.getInt("n");
        }
      }
      if (terminated > 0) {
        return;
      }
      Thread.sleep(200);
    }
    fail("could not find a LISTEN backend to terminate for channel " + channel);
  }
}
