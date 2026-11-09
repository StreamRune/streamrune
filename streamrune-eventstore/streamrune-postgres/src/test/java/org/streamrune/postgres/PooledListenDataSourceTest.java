package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

/** Every call of the {@link ListenDataSource} reaches the pool behind it. */
class PooledListenDataSourceTest {

  private final HikariDataSource pool = mock(HikariDataSource.class);
  private final PooledListenDataSource listen = new PooledListenDataSource(pool);

  @Test
  void theConnectionsComeFromThePool() throws SQLException {
    Connection first = mock(Connection.class);
    Connection second = mock(Connection.class);
    when(pool.getConnection()).thenReturn(first);
    when(pool.getConnection("user", "secret")).thenReturn(second);

    assertThat(listen.getConnection()).isSameAs(first);
    assertThat(listen.getConnection("user", "secret")).isSameAs(second);
  }

  @Test
  void theDataSourceSettingsAreThePools() throws Exception {
    var writer = new PrintWriter(new StringWriter());
    when(pool.getLogWriter()).thenReturn(writer);
    when(pool.getLoginTimeout()).thenReturn(9);
    Logger logger = Logger.getLogger("listen");
    when(pool.getParentLogger()).thenReturn(logger);

    listen.setLogWriter(writer);
    listen.setLoginTimeout(9);

    verify(pool).setLogWriter(writer);
    verify(pool).setLoginTimeout(9);
    assertThat(listen.getLogWriter()).isSameAs(writer);
    assertThat(listen.getLoginTimeout()).isEqualTo(9);
    assertThat(listen.getParentLogger()).isSameAs(logger);
  }

  @Test
  void unwrapHandsOutItselfThePoolOrWhatThePoolWraps() throws SQLException {
    Driver driver = mock(Driver.class);
    when(pool.unwrap(Driver.class)).thenReturn(driver);
    when(pool.isWrapperFor(Driver.class)).thenReturn(true);

    assertThat(listen.unwrap(ListenDataSource.class)).isSameAs(listen);
    assertThat(listen.unwrap(DataSource.class)).isSameAs(listen);
    assertThat(listen.unwrap(HikariDataSource.class)).isSameAs(pool);
    assertThat(listen.unwrap(Driver.class)).isSameAs(driver);
    assertThat(listen.isWrapperFor(ListenDataSource.class)).isTrue();
    assertThat(listen.isWrapperFor(HikariDataSource.class)).isTrue();
    assertThat(listen.isWrapperFor(Driver.class)).isTrue();
    assertThat(listen.isWrapperFor(Runnable.class)).isFalse();
  }

  @Test
  void closeClosesThePool_andToStringNamesIt() {
    when(pool.getPoolName()).thenReturn("streamrune-listen");

    listen.close();

    verify(pool).close();
    assertThat(listen).hasToString("ListenDataSource[streamrune-listen]");
  }

  @Test
  void aPoolIsRequired() {
    assertThatThrownBy(() -> new PooledListenDataSource(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("pool is required");
  }
}
