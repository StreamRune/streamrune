package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.ConnectionFailureClassifier;
import org.streamrune.core.EventStoreException;
import org.streamrune.test.EventStoreContract;

class SqliteConnectionFailuresTest {

  @TempDir Path dir;

  @ParameterizedTest
  @EnumSource(
      value = SQLiteErrorCode.class,
      names = {
        "SQLITE_CANTOPEN",
        "SQLITE_CANTOPEN_ISDIR",
        "SQLITE_NOTADB",
        "SQLITE_CORRUPT",
        "SQLITE_IOERR",
        "SQLITE_IOERR_READ",
        "SQLITE_IOERR_FSYNC",
        "SQLITE_FULL",
        "SQLITE_READONLY",
        "SQLITE_READONLY_DBMOVED"
      })
  void anUnusableFileIsAConnectionFailure_andOnlyThroughTheSqliteClassifier(SQLiteErrorCode code) {
    Throwable failure =
        new EventStoreException("a store's wrapper", new SQLiteException("x", code));

    assertThat(SqliteConnectionFailures.CLASSIFIER.isConnectionFailure(failure)).isTrue();
    assertThat(ConnectionFailureClassifier.GENERIC.isConnectionFailure(failure)).isFalse();
  }

  @ParameterizedTest
  @EnumSource(
      value = SQLiteErrorCode.class,
      names = {
        "SQLITE_BUSY",
        "SQLITE_BUSY_SNAPSHOT",
        "SQLITE_LOCKED",
        "SQLITE_CONSTRAINT",
        "SQLITE_CONSTRAINT_UNIQUE",
        "SQLITE_ERROR",
        "SQLITE_INTERRUPT",
        "SQLITE_TOOBIG"
      })
  void theFailureOfOneWriteIsNotAConnectionFailure(SQLiteErrorCode code) {
    Throwable failure =
        new EventStoreException("a store's wrapper", new SQLiteException("x", code));

    assertThat(SqliteConnectionFailures.CLASSIFIER.isConnectionFailure(failure)).isFalse();
  }

  @Test
  void theGenericRulesHold() {
    assertThat(
            SqliteConnectionFailures.CLASSIFIER.isConnectionFailure(new SQLException("x", "08006")))
        .isTrue();
    assertThat(SqliteConnectionFailures.CLASSIFIER.isConnectionFailure(null)).isFalse();
  }

  @Test
  void theSqliteSagaStoreContributesThem() {
    SqliteDatabase database =
        SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("classifier.db")));
    try {
      new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry())
          .create();
      var store = new SqliteSagaStore(database, new ObjectMapper());

      assertThat(store.connectionFailureClassifier()).isSameAs(SqliteConnectionFailures.CLASSIFIER);
    } finally {
      database.close();
    }
  }

  /**
   * Against the driver: a file that is not a database is a connection failure, the write lock not
   * granted within busy_timeout is not.
   */
  @Test
  void aFileThatIsNotADatabaseIsAConnectionFailure_aWriteLockTimeoutIsNot() throws Exception {
    Path garbage = dir.resolve("garbage.db");
    Files.write(garbage, "this file is not a SQLite database, whatever its name says".getBytes());
    SQLException notADatabase = null;
    try (Connection conn = DriverManager.getConnection("jdbc:sqlite:file:" + garbage);
        Statement st = conn.createStatement()) {
      st.executeQuery("SELECT count(*) FROM sqlite_master").close();
    } catch (SQLException e) {
      notADatabase = e;
    }
    assertThat((Throwable) notADatabase).isNotNull();
    assertThat(SqliteConnectionFailures.CLASSIFIER.isConnectionFailure(notADatabase)).isTrue();

    String url = "jdbc:sqlite:file:" + dir.resolve("busy.db") + "?busy_timeout=50";
    try (Connection holder = DriverManager.getConnection(url);
        Connection waiter = DriverManager.getConnection(url);
        Statement holding = holder.createStatement();
        Statement waiting = waiter.createStatement()) {
      holding.execute("BEGIN IMMEDIATE");
      SQLException busy = null;
      try {
        waiting.execute("BEGIN IMMEDIATE");
      } catch (SQLException e) {
        busy = e;
      }
      holding.execute("ROLLBACK");
      assertThat((Throwable) busy).isNotNull();
      assertThat(SqliteConnectionFailures.CLASSIFIER.isConnectionFailure(busy)).isFalse();
    }
  }
}
