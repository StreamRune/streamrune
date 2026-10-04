package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/** Unit tests for PgNotificationListener. */
class PgNotificationListenerTest {

  @Test
  void constructorThrowsOnNullDataSource() {
    assertThrows(IllegalArgumentException.class, () -> new PgNotificationListener(null, () -> {}));
  }

  @Test
  void constructorThrowsOnNullCallback() {
    var dataSource = new MockDataSource();
    assertThrows(
        IllegalArgumentException.class, () -> new PgNotificationListener(dataSource, null));
  }

  @Test
  void constructorThrowsOnNullChannel() {
    var dataSource = new MockDataSource();
    assertThrows(
        IllegalArgumentException.class,
        () -> new PgNotificationListener(dataSource, () -> {}, null));
  }

  @Test
  void constructorThrowsOnBlankChannel() {
    var dataSource = new MockDataSource();
    assertThrows(
        IllegalArgumentException.class,
        () -> new PgNotificationListener(dataSource, () -> {}, "   "));
  }

  @Test
  void constructorWithDefaultChannel() {
    var dataSource = new MockDataSource();
    var listener = new PgNotificationListener(dataSource, () -> {});

    assertFalse(listener.isRunning());
  }

  @Test
  void constructorWithCustomChannel() {
    var dataSource = new MockDataSource();
    var listener = new PgNotificationListener(dataSource, () -> {}, "custom_channel");

    assertFalse(listener.isRunning());
  }

  @Test
  void startThrowsWhenAlreadyRunning() {
    var dataSource = new MockDataSource();
    var listener = new PgNotificationListener(dataSource, () -> {});

    listener.start();
    assertTrue(listener.isRunning());

    assertThrows(IllegalStateException.class, listener::start);

    listener.close();
  }

  @Test
  void startAfterCloseThrows_noZombieResurrection() {
    // close() is terminal. The old single `running` flag was unset by close() BEFORE
    // the join, so a start() during that window (or after) CAS'd it back to true and resurrected a
    // second listener thread. With a NEW/RUNNING/CLOSED lifecycle, close() moves to the terminal
    // CLOSED state and start() can only leave NEW — so a post-close start() throws instead of
    // spawning a zombie.
    var dataSource = new MockDataSource();
    var listener = new PgNotificationListener(dataSource, () -> {});

    listener.start();
    listener.close();
    assertFalse(listener.isRunning());

    assertThrows(IllegalStateException.class, listener::start);
    assertFalse(listener.isRunning());
  }

  @Test
  void builderCreatesListener() {
    var dataSource = new MockDataSource();

    var listener =
        PgNotificationListener.builder()
            .dataSource(dataSource)
            .onNotification(() -> {})
            .channel("test_channel")
            .build();

    assertFalse(listener.isRunning());
  }

  @Test
  void builderUsesDefaultChannel() {
    var dataSource = new MockDataSource();

    var listener =
        PgNotificationListener.builder().dataSource(dataSource).onNotification(() -> {}).build();

    assertFalse(listener.isRunning());
  }

  @Test
  void builderThrowsOnMissingDataSource() {
    var builder = PgNotificationListener.builder().onNotification(() -> {});

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  @Test
  void builderThrowsOnMissingCallback() {
    var dataSource = new MockDataSource();

    var builder = PgNotificationListener.builder().dataSource(dataSource);

    assertThrows(IllegalArgumentException.class, builder::build);
  }

  /** Simple mock DataSource for testing. */
  private static class MockDataSource implements DataSource {
    @Override
    public java.sql.Connection getConnection() {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.sql.Connection getConnection(String username, String password) {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.io.PrintWriter getLogWriter() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLogWriter(java.io.PrintWriter out) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLoginTimeout(int seconds) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }
}
