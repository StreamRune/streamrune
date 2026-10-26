package org.streamrune.integration.tck;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.streamrune.integration.tck.IntegrationWiringContractTest.StubDataSource;

class RecordingDataSourceTest {

  @Test
  void itCountsTheConnectionsItGrantsAndRefusesEveryRequestOnceClosed() throws Exception {
    var dataSource = new RecordingDataSource(new StubDataSource());
    assertNotNull(dataSource.getConnection());
    assertNotNull(dataSource.getConnection("user", "secret"));
    assertEquals(2, dataSource.connectionsGranted());
    assertFalse(dataSource.isClosed());

    dataSource.close();
    dataSource.close();

    assertTrue(dataSource.isClosed());
    assertThrows(SQLException.class, dataSource::getConnection);
    assertThrows(SQLException.class, () -> dataSource.getConnection("user", "secret"));
    String thread = Thread.currentThread().getName();
    assertEquals(List.of(thread, thread), dataSource.requestsAfterClose());
    assertEquals(2, dataSource.connectionsGranted());
  }

  @Test
  void itCountsTheConnectionsStillInUseWhenItCloses() throws Exception {
    var dataSource = new RecordingDataSource(new StubDataSource());
    var returned = dataSource.getConnection();
    var kept = dataSource.getConnection("user", "secret");
    returned.close();
    returned.close();
    assertEquals(-1, dataSource.connectionsInUseAtClose());

    dataSource.close();

    assertEquals(1, dataSource.connectionsInUseAtClose());
    kept.close();
    assertEquals(1, dataSource.connectionsInUseAtClose());
  }

  @Test
  void aConnectionItHandsOutIsItselfAndPassesEverythingElseOn() throws Exception {
    var dataSource = new RecordingDataSource(new StubDataSource());
    var connection = dataSource.getConnection();
    var other = dataSource.getConnection();
    assertEquals(connection, connection);
    assertNotEquals(connection, other);
    assertEquals(System.identityHashCode(connection), connection.hashCode());
    assertFalse(connection.isClosed());
    assertNull(connection.getSchema());
  }

  /** A connection whose calls fail, as a broken one's do. */
  @Test
  void aFailingCallOfAConnectionFailsWithItsOwnException() throws Exception {
    var failing =
        new StubDataSource() {
          @Override
          public java.sql.Connection getConnection() {
            return (java.sql.Connection)
                java.lang.reflect.Proxy.newProxyInstance(
                    java.sql.Connection.class.getClassLoader(),
                    new Class<?>[] {java.sql.Connection.class},
                    (proxy, method, args) -> {
                      throw new SQLException("broken");
                    });
          }
        };
    var connection = new RecordingDataSource(failing).getConnection();
    var thrown = assertThrows(SQLException.class, connection::getSchema);
    assertEquals("broken", thrown.getMessage());
  }

  /** A delegate that is closeable, like a connection pool. */
  static final class ClosingDelegate extends StubDataSource implements AutoCloseable {
    final AtomicBoolean closed = new AtomicBoolean();

    @Override
    public void close() {
      closed.set(true);
    }
  }

  @Test
  void closingItClosesACloseableDelegate() throws Exception {
    var delegate = new ClosingDelegate();
    new RecordingDataSource(delegate).close();
    assertTrue(delegate.closed.get());
  }

  @Test
  void itPassesEverythingElseToTheDelegateAndUnwrapsToItself() throws Exception {
    var dataSource = new RecordingDataSource(new StubDataSource());
    var log = new PrintWriter(new StringWriter());
    dataSource.setLogWriter(log);
    dataSource.setLoginTimeout(3);
    assertEquals(null, dataSource.getLogWriter());
    assertEquals(0, dataSource.getLoginTimeout());
    assertNotNull(dataSource.getParentLogger());
    assertSame(dataSource, dataSource.unwrap(RecordingDataSource.class));
    assertTrue(dataSource.isWrapperFor(RecordingDataSource.class));
    assertFalse(dataSource.isWrapperFor(String.class));
    assertThrows(UnsupportedOperationException.class, () -> dataSource.unwrap(String.class));
  }

  @Test
  void itNeedsADelegate() {
    assertThrows(IllegalArgumentException.class, () -> new RecordingDataSource(null));
  }
}
