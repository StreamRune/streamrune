package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.FileInputStream;
import java.lang.reflect.Proxy;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.EventStoreException;

class SqliteDatabaseTest {

  @TempDir Path dir;

  @ParameterizedTest
  @ValueSource(
      strings = {
        "jdbc:sqlite::memory:",
        "jdbc:sqlite:file:x?mode=memory",
        "jdbc:sqlite:",
        "jdbc:sqlite:file::memory:?cache=shared"
      })
  void inMemoryUrlsAreRejected(String url) {
    assertThatThrownBy(() -> SqliteDatabase.open(TestDatabases.url(url)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("in-memory")
        .hasMessageContaining(url);
  }

  @ParameterizedTest(name = "{0}")
  @CsvSource(
      delimiter = '|',
      value = {
        "journal_mode=DELETE | journal_mode | journal_mode=WAL",
        "synchronous=NORMAL | synchronous | synchronous=FULL",
        "synchronous=OFF | synchronous | synchronous=FULL",
        "foreign_keys=false | foreign_keys | foreign_keys=ON",
        "busy_timeout=0 | busy_timeout | busy_timeout greater than 0",
        "transaction_mode=DEFERRED | transaction_mode | transaction_mode=IMMEDIATE",
        "transaction_mode=EXCLUSIVE | transaction_mode | transaction_mode=IMMEDIATE"
      })
  void wrongSetupIsRefusedNamingTheSetting(String override, String setting, String expected) {
    Path db = dir.resolve("db.sqlite");

    assertThatThrownBy(() -> SqliteDatabase.open(TestDatabases.dataSource(db, override)))
        .isInstanceOf(IllegalStateException.class)
        .isNotInstanceOf(SqliteGuardException.class)
        .hasMessageContaining(setting)
        .hasMessageContaining(expected);
  }

  @Test
  void wrongSetupLeavesNoGuardReference() throws Exception {
    Path db = dir.resolve("db.sqlite");

    assertThatThrownBy(
            () -> SqliteDatabase.open(TestDatabases.dataSource(db, "foreign_keys=false")))
        .isInstanceOf(IllegalStateException.class);

    assertThat(Files.exists(lockFileOf(db))).isFalse();
    assertThat(ForkedJvm.probe(db)).startsWith(GuardChild.ACQUIRED);
  }

  /**
   * A pool that hands out connections in manual-commit mode holds the write lock with every idle
   * connection: refused at open, before the guard, with the reason.
   */
  @Test
  void aManualCommitPoolIsRefusedBeforeTheGuard() throws Exception {
    Path db = dir.resolve("db.sqlite");
    com.zaxxer.hikari.HikariConfig config = new com.zaxxer.hikari.HikariConfig();
    config.setJdbcUrl("jdbc:sqlite:file:" + db + "?" + TestDatabases.REQUIRED_SETTINGS);
    config.setAutoCommit(false);
    config.setMaximumPoolSize(1);
    try (var pool = new com.zaxxer.hikari.HikariDataSource(config)) {
      assertThatThrownBy(() -> SqliteDatabase.open(pool))
          .isExactlyInstanceOf(IllegalStateException.class)
          .hasMessageContaining("autoCommit=false")
          .hasMessageContaining("write lock");
    }
    assertThat(Files.exists(lockFileOf(db))).isFalse();
  }

  @Test
  void synchronousExtraIsAccepted() {
    Path db = dir.resolve("db.sqlite");

    try (SqliteDatabase database =
        SqliteDatabase.open(TestDatabases.dataSource(db, "synchronous=EXTRA"))) {
      assertThat(database.file()).isEqualTo(realPath(db));
    }
  }

  @Test
  void openReturnsTheDataSourceAndTheCanonicalFile() {
    Path db = dir.resolve("db.sqlite");
    DataSource dataSource = TestDatabases.dataSource(db);

    try (SqliteDatabase database = SqliteDatabase.open(dataSource)) {
      assertThat(database.dataSource()).isSameAs(dataSource);
      assertThat(database.file()).isEqualTo(realPath(db)).isAbsolute();
      assertThat(database.toString()).contains(database.file().toString());
      assertThat(Files.exists(lockFileOf(db))).isTrue();
    }
  }

  @Test
  void secondHandleOnSameFileSharesTheGuard() throws Exception {
    Path db = dir.resolve("db.sqlite");

    try (SqliteDatabase first = SqliteDatabase.open(TestDatabases.dataSource(db));
        SqliteDatabase second = SqliteDatabase.open(TestDatabases.dataSource(db))) {
      assertThat(second.file()).isEqualTo(first.file());
      assertThat(InstanceGuardRegistry.references(lockFileOf(db))).isEqualTo(2);
      assertThat(ForkedJvm.probe(db)).startsWith(GuardChild.REFUSED);
    }
    assertThat(InstanceGuardRegistry.references(lockFileOf(db))).isZero();
  }

  @Test
  void lockReleasedOnlyWhenLastHandleCloses() throws Exception {
    Path db = dir.resolve("db.sqlite");
    SqliteDatabase first = SqliteDatabase.open(TestDatabases.dataSource(db));
    SqliteDatabase second = SqliteDatabase.open(TestDatabases.dataSource(db));

    first.close();
    first.close();

    assertThat(InstanceGuardRegistry.references(lockFileOf(db))).isEqualTo(1);
    assertThat(ForkedJvm.probe(db)).startsWith(GuardChild.REFUSED);

    second.close();

    assertThat(InstanceGuardRegistry.references(lockFileOf(db))).isZero();
    assertThat(ForkedJvm.probe(db)).startsWith(GuardChild.ACQUIRED);
  }

  @Test
  void lockSurvivesAnotherStreamOnTheLockFileBeingClosed() throws Exception {
    Path db = dir.resolve("db.sqlite");

    try (SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(db))) {
      new FileInputStream(lockFileOf(db).toFile()).close();
      Files.newByteChannel(lockFileOf(db)).close();

      assertThat(ForkedJvm.probe(db))
          .startsWith(GuardChild.REFUSED)
          .contains(database.file().toString());
    }
  }

  @Test
  void sameFileDifferentSpellingsShareOneGuard() throws Exception {
    Path db = dir.resolve("db.sqlite");
    Path link = Files.createSymbolicLink(dir.resolve("link.sqlite"), db);
    Path workingDirectory = Path.of("").toAbsolutePath();
    assumeThat(workingDirectory.getRoot()).isEqualTo(db.toAbsolutePath().getRoot());
    Path relative = workingDirectory.relativize(db.toAbsolutePath());
    List<String> urls =
        List.of(
            "jdbc:sqlite:" + db.toAbsolutePath() + "?" + TestDatabases.REQUIRED_SETTINGS,
            "jdbc:sqlite:file:" + db.toAbsolutePath() + "?" + TestDatabases.REQUIRED_SETTINGS,
            "jdbc:sqlite:" + relative + "?" + TestDatabases.REQUIRED_SETTINGS,
            "jdbc:sqlite:./" + relative + "?" + TestDatabases.REQUIRED_SETTINGS,
            "jdbc:sqlite:"
                + dir.toAbsolutePath()
                + "/./db.sqlite?"
                + TestDatabases.REQUIRED_SETTINGS,
            "jdbc:sqlite:file:" + link + "?" + TestDatabases.REQUIRED_SETTINGS);
    List<SqliteDatabase> handles = new ArrayList<>();
    try {
      for (String url : urls) {
        handles.add(SqliteDatabase.open(TestDatabases.url(url)));
      }

      assertThat(handles).extracting(SqliteDatabase::file).containsOnly(realPath(db));
      assertThat(InstanceGuardRegistry.references(lockFileOf(db))).isEqualTo(urls.size());
      assertThat(Files.exists(dir.resolve("link.sqlite.lock"))).isFalse();
    } finally {
      handles.forEach(SqliteDatabase::close);
    }
    assertThat(InstanceGuardRegistry.references(lockFileOf(db))).isZero();
  }

  @Test
  void concurrentOpensOnOneFileShareOneGuard() throws Exception {
    Path db = dir.resolve("db.sqlite");
    // Create the database first, so that the threads race on the guard and not on WAL setup.
    SqliteDatabase.open(TestDatabases.dataSource(db)).close();
    int threads = 8;
    CyclicBarrier barrier = new CyclicBarrier(threads);
    List<SqliteDatabase> handles = new ArrayList<>();
    try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
      List<Future<SqliteDatabase>> opened = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        opened.add(
            pool.submit(
                () -> {
                  barrier.await(ForkedJvm.TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                  return SqliteDatabase.open(TestDatabases.dataSource(db));
                }));
      }
      for (Future<SqliteDatabase> handle : opened) {
        handles.add(handle.get(ForkedJvm.TIMEOUT.toSeconds(), TimeUnit.SECONDS));
      }

      assertThat(handles).extracting(SqliteDatabase::file).containsOnly(realPath(db));
      assertThat(InstanceGuardRegistry.references(lockFileOf(db))).isEqualTo(threads);
    } finally {
      handles.forEach(SqliteDatabase::close);
    }
    assertThat(InstanceGuardRegistry.references(lockFileOf(db))).isZero();
  }

  @Test
  void unwritableLockDirectoryFailsWithLockPath() throws Exception {
    assumeThat(FileSystems.getDefault().supportedFileAttributeViews()).contains("posix");
    Path sub = Files.createDirectory(dir.resolve("read-only"));
    Path db = sub.resolve("db.sqlite");
    DataSource dataSource = TestDatabases.dataSource(db);
    // An open connection keeps the -wal and -shm files, so SQLite itself still opens the database
    // once the directory is read-only; only the lock file cannot be created.
    try (Connection held = dataSource.getConnection()) {
      held.createStatement().execute("CREATE TABLE t (x INTEGER)");
      Files.setPosixFilePermissions(sub, PosixFilePermissions.fromString("r-xr-xr-x"));
      try {
        assumeThat(Files.isWritable(sub)).as("the test user can write a read-only dir").isFalse();

        assertThatThrownBy(() -> SqliteDatabase.open(dataSource))
            .isInstanceOf(SqliteGuardException.class)
            .hasMessageContaining(realPath(sub).resolve("db.sqlite.lock").toString())
            .hasMessageContaining("AccessDeniedException")
            .hasCauseInstanceOf(java.nio.file.AccessDeniedException.class);
      } finally {
        Files.setPosixFilePermissions(sub, PosixFilePermissions.fromString("rwxr-xr-x"));
      }
    }
  }

  @Test
  void hardLinkedDatabaseIsRefused() throws Exception {
    assumeThat(FileSystems.getDefault().supportedFileAttributeViews()).contains("unix");
    Path db = dir.resolve("db.sqlite");
    SqliteDatabase.open(TestDatabases.dataSource(db)).close();
    Path hard = Files.createLink(dir.resolve("hard.sqlite"), db);

    for (Path name : List.of(db, hard)) {
      assertThatThrownBy(() -> SqliteDatabase.open(TestDatabases.dataSource(name)))
          .isInstanceOf(SqliteGuardException.class)
          .hasMessageContaining("hard links")
          .hasMessageContaining(realPath(name).toString());
    }
    assertThat(Files.exists(dir.resolve("hard.sqlite.lock"))).isFalse();
  }

  @Test
  void attachedDatabasesAreRefused() {
    Path db = dir.resolve("db.sqlite");
    Path other = dir.resolve("other.sqlite");
    DataSource attaching =
        TestDatabases.withInitSql(
            "jdbc:sqlite:" + db + "?" + TestDatabases.REQUIRED_SETTINGS,
            "ATTACH DATABASE '" + other + "' AS other");

    assertThatThrownBy(() -> SqliteDatabase.open(attaching))
        .isInstanceOf(IllegalStateException.class)
        .isNotInstanceOf(SqliteGuardException.class)
        .hasMessageContaining("attached")
        .hasMessageContaining("other.sqlite");
    assertThat(Files.exists(lockFileOf(db))).isFalse();
  }

  @Test
  void theTempDatabaseIsAllowed() {
    Path db = dir.resolve("db.sqlite");
    DataSource withTemp =
        TestDatabases.withInitSql(
            "jdbc:sqlite:" + db + "?" + TestDatabases.REQUIRED_SETTINGS,
            "CREATE TEMP TABLE scratch (x INTEGER)");

    try (SqliteDatabase database = SqliteDatabase.open(withTemp)) {
      assertThat(database.file()).isEqualTo(realPath(db));
    }
  }

  @Test
  void openCloseChurnLeavesNoReference() throws Exception {
    Path db = dir.resolve("db.sqlite");
    SqliteDatabase.open(TestDatabases.dataSource(db)).close();
    int threads = 6;
    int rounds = 40;
    CyclicBarrier barrier = new CyclicBarrier(threads);
    try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
      List<Future<?>> loops = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        loops.add(
            pool.submit(
                () -> {
                  barrier.await(ForkedJvm.TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                  for (int round = 0; round < rounds; round++) {
                    SqliteDatabase.open(TestDatabases.dataSource(db)).close();
                  }
                  return null;
                }));
      }
      for (Future<?> loop : loops) {
        loop.get(ForkedJvm.TIMEOUT.toSeconds(), TimeUnit.SECONDS);
      }
    }

    assertThat(InstanceGuardRegistry.references(lockFileOf(db))).isZero();
    assertThat(ForkedJvm.probe(db)).startsWith(GuardChild.ACQUIRED);
  }

  @Test
  void olderSqliteIsRefused() {
    assertThatThrownBy(() -> SqliteVersion.require("3.44.0"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("3.44.0")
        .hasMessageContaining("3.45.0");
  }

  @ParameterizedTest
  @ValueSource(strings = {"3.45.0", "3.45.1", "3.54.1", "3.100.0", "4.0.0"})
  void supportedSqliteIsAccepted(String version) {
    assertThatCode(() -> SqliteVersion.require(version)).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(strings = {"3.44.9", "3.9.99", "2.99.99"})
  void everyOlderSqliteIsRefused(String version) {
    assertThatThrownBy(() -> SqliteVersion.require(version))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(version);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "3.45", "3.x.0", "3.45.0.1", "three"})
  void unreadableSqliteVersionIsRefused(String version) {
    assertThatThrownBy(() -> SqliteVersion.require(version))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("cannot read");
  }

  @Test
  void nullDataSourceIsRejected() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> SqliteDatabase.open(null))
        .withMessageContaining("dataSource");
  }

  @Test
  void unreachableDatabaseFailsAsEventStoreException() {
    DataSource broken =
        dataSource(
            (proxy, method, args) -> {
              throw new SQLException("connection refused");
            });

    assertThatThrownBy(() -> SqliteDatabase.open(broken))
        .isInstanceOf(EventStoreException.class)
        .hasMessageContaining("connection refused")
        .hasCauseInstanceOf(SQLException.class);
  }

  @Test
  void dataSourceOfAnotherDatabaseIsRejected() {
    Connection notSqlite =
        (Connection)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) ->
                    switch (method.getName()) {
                      case "isWrapperFor" -> false;
                      case "close" -> null;
                      default -> throw new UnsupportedOperationException(method.getName());
                    });

    assertThatThrownBy(() -> SqliteDatabase.open(dataSource((proxy, method, args) -> notSqlite)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("not a SQLite connection");
  }

  private static DataSource dataSource(java.lang.reflect.InvocationHandler handler) {
    return (DataSource)
        Proxy.newProxyInstance(
            SqliteDatabaseTest.class.getClassLoader(), new Class<?>[] {DataSource.class}, handler);
  }

  static Path lockFileOf(Path db) {
    Path real = realPath(db);
    return real.resolveSibling(real.getFileName() + ".lock");
  }

  static Path realPath(Path path) {
    try {
      return path.toRealPath();
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }
}
