package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.EventStoreContract.ContractState;

/**
 * The in-process write mutex of {@link SqliteTransactions}: writers of one database file queue for
 * it in Java, first come first served, instead of waiting for SQLite's write lock inside the
 * driver; the wait is bounded by the connection's {@code busy_timeout} and fails as SQLite's own
 * refusal does; the holder's thread passes it; it is released on every exit; and a connection whose
 * file cannot be told takes none.
 *
 * <p>Where a thread waits is read from its stack: a writer queued on the mutex is in {@code
 * awaitWriteLock} with no frame of the driver, a writer waiting for SQLite's lock is inside the
 * driver's native call.
 */
@Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class SqliteWriteMutexTest {

  /** The busy_timeout of the connections that are meant to give up. */
  static final int IMPATIENT_MILLIS = 300;

  /** What the mutex's own timeout says, where SQLite's says "database is locked". */
  static final String MUTEX_TIMEOUT = "another write transaction of this process";

  @TempDir Path dir;

  private Path file;
  private DataSource patient;
  private DataSource impatient;
  private final ExecutorService threads = Executors.newCachedThreadPool();
  private final List<AutoCloseable> closeables = new ArrayList<>();

  @BeforeEach
  void open() throws SQLException {
    file = dir.resolve("mutex.db");
    patient = TestDatabases.dataSource(file);
    impatient = TestDatabases.dataSource(file, "busy_timeout=" + IMPATIENT_MILLIS);
    execute(
        patient,
        "CREATE TABLE side (id TEXT NOT NULL PRIMARY KEY)",
        "CREATE TABLE parent (id INTEGER PRIMARY KEY)",
        "CREATE TABLE child (parent INTEGER REFERENCES parent (id) DEFERRABLE INITIALLY DEFERRED)");
  }

  @AfterEach
  void close() throws Exception {
    threads.shutdownNow();
    for (AutoCloseable closeable : closeables) {
      closeable.close();
    }
  }

  private static void execute(DataSource dataSource, String... statements) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      for (String sql : statements) {
        st.execute(sql);
      }
    }
  }

  private static void insert(Connection conn, String id) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute("INSERT INTO side (id) VALUES ('" + id + "')");
    }
  }

  private List<String> rows() throws SQLException {
    var ids = new ArrayList<String>();
    try (Connection conn = patient.getConnection();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("SELECT id FROM side ORDER BY id")) {
      while (rs.next()) {
        ids.add(rs.getString(1));
      }
    }
    return ids;
  }

  private static void awaitQuietly(CountDownLatch latch) {
    try {
      latch.await(60, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** One store write of {@code id} through {@code dataSource}, as every store runs its writes. */
  private static void write(DataSource dataSource, String id) {
    SqliteTransactions.write(
        dataSource,
        "test write",
        "write of " + id,
        "Failed to write " + id,
        conn -> {
          insert(conn, id);
          return null;
        });
  }

  /** A write transaction held open on a thread of its own until {@link #release()}. */
  private final class Holder implements AutoCloseable {
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private final Future<?> done;

    Holder(DataSource dataSource, String id) throws InterruptedException {
      done =
          threads.submit(
              () -> {
                try (Connection conn = dataSource.getConnection()) {
                  return SqliteTransactions.inTransaction(
                      conn,
                      "holder",
                      c -> {
                        insert(c, id);
                        entered.countDown();
                        awaitQuietly(release);
                        return null;
                      });
                }
              });
      assertThat(entered.await(30, TimeUnit.SECONDS)).as("the holder began").isTrue();
    }

    void release() throws Exception {
      release.countDown();
      done.get(30, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
      release.countDown();
    }
  }

  /** A thread that runs {@code work}, so that a test can see where it waits. */
  private static final class Writer {
    private final Thread thread;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicBoolean interruptedAfterwards = new AtomicBoolean();

    Writer(String name, Runnable work) {
      thread =
          Thread.ofPlatform()
              .name(name)
              .daemon()
              .start(
                  () -> {
                    try {
                      work.run();
                    } catch (Throwable t) {
                      failure.set(t);
                    }
                    interruptedAfterwards.set(Thread.currentThread().isInterrupted());
                  });
    }

    /** Waits until the thread is parked in {@code awaitWriteLock}, outside the driver. */
    void awaitQueuedOnTheMutex() throws InterruptedException {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (!queuedOnTheMutex()) {
        assertThat(thread.isAlive()).as("%s ended before it queued", thread.getName()).isTrue();
        assertThat(System.nanoTime())
            .as("%s queued in time", thread.getName())
            .isLessThan(deadline);
        Thread.sleep(5);
      }
    }

    boolean queuedOnTheMutex() {
      StackTraceElement[] stack = thread.getStackTrace();
      boolean inAwait =
          Arrays.stream(stack)
              .anyMatch(
                  frame ->
                      frame.getClassName().equals(SqliteTransactions.class.getName())
                          && frame.getMethodName().equals("awaitWriteLock"));
      boolean inDriver =
          Arrays.stream(stack).anyMatch(frame -> frame.getClassName().startsWith("org.sqlite."));
      return inAwait && !inDriver && thread.getState() == Thread.State.TIMED_WAITING;
    }

    void join() throws InterruptedException {
      thread.join(TimeUnit.SECONDS.toMillis(30));
      assertThat(thread.isAlive()).as("%s ended", thread.getName()).isFalse();
    }
  }

  /** The write lock and the mutex are free: an impatient write from another thread commits. */
  private void assertFreeForAnotherThread(String id) throws Exception {
    long start = System.nanoTime();
    threads.submit(() -> write(impatient, id)).get(30, TimeUnit.SECONDS);
    assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(IMPATIENT_MILLIS);
  }

  // ---- writers queue in Java ----

  @Test
  void aWriterBehindAnOpenTransactionWaitsInJavaAndCommitsOnceItEnds() throws Exception {
    try (Holder holder = new Holder(patient, "held")) {
      var writer = new Writer("writer", () -> write(patient, "queued"));

      writer.awaitQueuedOnTheMutex();
      assertThat(rows()).as("a read does not wait, and nothing is committed yet").isEmpty();
      holder.release();
      writer.join();

      assertThat(writer.failure.get()).isNull();
    }
    assertThat(rows()).containsExactly("held", "queued");
  }

  @Test
  void writersAreServedInTheOrderTheyArrived() throws Exception {
    List<String> served = Collections.synchronizedList(new ArrayList<>());
    var writers = new ArrayList<Writer>();
    try (Holder holder = new Holder(patient, "held")) {
      for (String id : List.of("first", "second", "third", "fourth")) {
        var writer =
            new Writer(
                id,
                () ->
                    SqliteTransactions.write(
                        patient,
                        "test write",
                        "write of " + id,
                        "Failed to write " + id,
                        conn -> {
                          served.add(id);
                          insert(conn, id);
                          return null;
                        }));
        writer.awaitQueuedOnTheMutex();
        writers.add(writer);
      }
      holder.release();
      for (Writer writer : writers) {
        writer.join();
        assertThat(writer.failure.get()).isNull();
      }
    }
    assertThat(served).containsExactly("first", "second", "third", "fourth");
  }

  /**
   * Every pool and every URL spelling of one file shares the mutex: a holder on one URL, a writer
   * on another URL of the same file, and one through a symbolic link, all queue on it. A writer on
   * another file does not.
   */
  @Test
  void everySpellingOfOneFileSharesTheMutexAndAnotherFileDoesNot() throws Exception {
    Path link = dir.resolve("link-to-mutex.db");
    boolean linked = true;
    try {
      Files.createSymbolicLink(link, file);
    } catch (UnsupportedOperationException | java.io.IOException e) {
      linked = false;
    }
    DataSource elsewhere = TestDatabases.dataSource(dir.resolve("elsewhere.db"));
    execute(elsewhere, "CREATE TABLE side (id TEXT NOT NULL PRIMARY KEY)");

    try (Holder holder = new Holder(patient, "held")) {
      var otherUrl = new Writer("other-url", () -> write(impatient, "other-url"));
      otherUrl.awaitQueuedOnTheMutex();
      Writer throughLink = null;
      if (linked) {
        DataSource viaLink = TestDatabases.dataSource(link);
        throughLink = new Writer("through-link", () -> write(viaLink, "through-link"));
        throughLink.awaitQueuedOnTheMutex();
      }

      long start = System.nanoTime();
      write(elsewhere, "another file");
      assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(IMPATIENT_MILLIS);

      holder.release();
      otherUrl.join();
      if (throughLink != null) {
        throughLink.join();
        assertThat(throughLink.failure.get()).isNull();
      }
    }
    assertThat(rows()).contains("held");
  }

  // ---- the bound and the failure ----

  /**
   * A writer that waits longer than its connection's busy_timeout fails with what SQLite itself
   * reports for a lock not granted, so every store reports it as before: the lock-failure message
   * naming the write, caused by SQLITE_BUSY.
   */
  @Test
  void aWriterThatWaitsLongerThanBusyTimeoutFailsAsSqlitesOwnRefusalDoes() throws Exception {
    try (Holder holder = new Holder(patient, "held")) {
      long start = System.nanoTime();
      assertThatThrownBy(() -> write(impatient, "late"))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessage(SqliteEventStore.busyMessage("write of late"))
          .cause()
          .isInstanceOf(SQLiteException.class)
          .hasMessageContaining("SQLITE_BUSY")
          .hasMessageContaining(MUTEX_TIMEOUT)
          .hasMessageContaining("busy_timeout of " + IMPATIENT_MILLIS + " ms")
          .satisfies(
              e ->
                  assertThat(((SQLiteException) e).getResultCode())
                      .isEqualTo(SQLiteErrorCode.SQLITE_BUSY));
      long waited = (System.nanoTime() - start) / 1_000_000;
      assertThat(waited).isBetween(IMPATIENT_MILLIS - 20L, IMPATIENT_MILLIS + 2_000L);

      holder.release();
    }
    assertThat(rows()).containsExactly("held");
    assertFreeForAnotherThread("after");
  }

  /**
   * The thread that holds the mutex passes it: a write it makes on a second connection reaches
   * SQLite, which refuses it after busy_timeout because the thread's own transaction holds the
   * write lock. The outer transaction still commits, and its mutex is released once.
   */
  @Test
  void theHoldersOwnThreadPassesTheMutexAndSqliteDecides() throws Exception {
    var refusal = new AtomicReference<Throwable>();

    try (Connection conn = patient.getConnection()) {
      SqliteTransactions.inTransaction(
          conn,
          "outer",
          c -> {
            insert(c, "outer");
            try {
              write(impatient, "nested");
            } catch (EventStoreException e) {
              refusal.set(e.getCause());
            }
            return null;
          });
    }

    assertThat(refusal.get())
        .isInstanceOf(SQLiteException.class)
        .hasMessageContaining("SQLITE_BUSY")
        .hasMessageNotContaining(MUTEX_TIMEOUT);
    assertThat(rows()).containsExactly("outer");
    assertFreeForAnotherThread("after");
  }

  // ---- released on every exit ----

  @Test
  void theMutexIsReleasedWhenTheWorkThrows() throws Exception {
    var boom = new IllegalStateException("the work fails");
    try (Connection conn = patient.getConnection()) {
      assertThatThrownBy(
              () ->
                  SqliteTransactions.inTransaction(
                      conn,
                      "failing",
                      c -> {
                        insert(c, "rolled back");
                        throw boom;
                      }))
          .isSameAs(boom);
      assertThatThrownBy(
              () ->
                  SqliteTransactions.inTransaction(
                      conn,
                      "failing",
                      c -> {
                        insert(c, "rolled back");
                        throw new SQLException("a statement fails");
                      }))
          .isExactlyInstanceOf(SQLException.class);
    }
    assertFreeForAnotherThread("after");
    assertThat(rows()).containsExactly("after");
  }

  @Test
  void theMutexIsReleasedWhenTheBeginFails() throws Exception {
    try (Connection conn = patient.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("BEGIN");
      assertThatThrownBy(() -> SqliteTransactions.inTransaction(conn, "refused", c -> null))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("cannot start a transaction within a transaction");
      st.execute("ROLLBACK");
    }
    assertFreeForAnotherThread("after");
  }

  @Test
  void theMutexIsReleasedWhenTheCommitFails() throws Exception {
    try (Connection conn = patient.getConnection()) {
      assertThatThrownBy(
              () ->
                  SqliteTransactions.inTransaction(
                      conn,
                      "orphan",
                      c -> {
                        try (Statement st = c.createStatement()) {
                          st.execute("INSERT INTO child (parent) VALUES (42)");
                        }
                        return null;
                      }))
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("FOREIGN KEY");
    }
    assertFreeForAnotherThread("after");
  }

  /** A connection in manual-commit mode is refused before any wait, and nothing is held. */
  @Test
  void aManualCommitConnectionIsRefusedBeforeTheMutexIsAskedFor() throws Exception {
    try (Holder holder = new Holder(patient, "held");
        Connection conn = DriverManager.getConnection("jdbc:sqlite:file:" + file)) {
      conn.setAutoCommit(false);
      long start = System.nanoTime();

      assertThatThrownBy(() -> SqliteTransactions.awaitWriteLock(conn))
          .isExactlyInstanceOf(IllegalStateException.class)
          .hasMessage(SqliteTransactions.MANUAL_COMMIT_CONNECTION);

      assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(IMPATIENT_MILLIS);
      holder.release();
    }
    assertFreeForAnotherThread("after");
  }

  /** A hold is released once: closing it again does not release a hold the thread still has. */
  @Test
  void closingAHoldTwiceReleasesItOnce() throws Exception {
    try (Connection first = patient.getConnection();
        Connection second = patient.getConnection()) {
      SqliteTransactions.WriteLock outer = SqliteTransactions.awaitWriteLock(first);
      SqliteTransactions.WriteLock inner = SqliteTransactions.awaitWriteLock(second);
      assertThat(outer.file()).isEqualTo(file.toRealPath());
      inner.close();
      inner.close();

      var writer = new Writer("writer", () -> write(patient, "queued"));
      writer.awaitQueuedOnTheMutex();

      outer.close();
      writer.join();
      assertThat(writer.failure.get()).isNull();
    }
  }

  // ---- interrupts ----

  /** As the wait for SQLite's own lock, the wait for the mutex is not ended by an interrupt. */
  @Test
  void anInterruptDoesNotEndTheWaitAndStaysSet() throws Exception {
    try (Holder holder = new Holder(patient, "held")) {
      var writer = new Writer("writer", () -> write(patient, "queued"));
      writer.awaitQueuedOnTheMutex();

      writer.thread.interrupt();
      Thread.sleep(100);
      writer.awaitQueuedOnTheMutex();

      holder.release();
      writer.join();
      assertThat(writer.failure.get()).isNull();
      assertThat(writer.interruptedAfterwards).isTrue();
    }
    assertThat(rows()).containsExactly("held", "queued");
  }

  @Test
  void aThreadThatArrivesInterruptedStillWrites() throws Exception {
    var interruptedAfterwards = new AtomicBoolean();
    threads
        .submit(
            () -> {
              Thread.currentThread().interrupt();
              write(patient, "interrupted");
              interruptedAfterwards.set(Thread.interrupted());
            })
        .get(30, TimeUnit.SECONDS);

    assertThat(interruptedAfterwards).isTrue();
    assertThat(rows()).containsExactly("interrupted");
  }

  // ---- no file, no mutex ----

  /** An in-memory database has no file to key a mutex on: its writes take none, and work. */
  @Test
  void anInMemoryDatabaseTakesNoMutex() throws Exception {
    try (Connection conn = DriverManager.getConnection("jdbc:sqlite::memory:")) {
      try (SqliteTransactions.WriteLock hold = SqliteTransactions.awaitWriteLock(conn)) {
        assertThat(hold.file()).isNull();
      }
      int one =
          SqliteTransactions.inTransaction(
              conn,
              "in memory",
              c -> {
                try (Statement st = c.createStatement()) {
                  st.execute("CREATE TABLE t (x)");
                }
                return 1;
              });
      assertThat(one).isEqualTo(1);
    }
  }

  /**
   * A connection that does not unwrap to the driver's takes no mutex either: the write runs, and
   * SQLite's own lock and busy_timeout serialize it, as before.
   */
  @Test
  void aConnectionThatIsNotTheDriversTakesNoMutex() throws Exception {
    try (Connection real = patient.getConnection()) {
      Connection opaque =
          (Connection)
              Proxy.newProxyInstance(
                  Connection.class.getClassLoader(),
                  new Class<?>[] {Connection.class},
                  (proxy, method, args) -> {
                    if (method.getName().equals("isWrapperFor")) {
                      return false;
                    }
                    try {
                      return method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                      throw e.getCause();
                    }
                  });
      try (SqliteTransactions.WriteLock hold = SqliteTransactions.awaitWriteLock(opaque)) {
        assertThat(hold.file()).isNull();
      }
      SqliteTransactions.inTransaction(
          opaque,
          "opaque",
          c -> {
            insert(c, "opaque");
            return null;
          });
    }
    assertThat(rows()).containsExactly("opaque");
  }

  // ---- the writes that are one statement ----

  /**
   * A snapshot save and a snapshot purge are one statement in autocommit, with no BEGIN: they queue
   * on the mutex too, and run once the open transaction ends.
   */
  @Test
  void aSnapshotSaveAndAPurgeQueueOnTheMutex() throws Exception {
    Path storeFile = dir.resolve("store.db");
    DataSource storeSource = TestDatabases.dataSource(storeFile);
    SqliteDatabase database = SqliteDatabase.open(storeSource);
    closeables.add(database);
    EventStore store =
        new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry())
            .create();
    StreamId stream = StreamId.of(AggregateType.of("order"), AggregateId.of("snap"));
    var purger =
        new SqliteSnapshotStorePurger(database, subject -> List.of(stream), Clock.systemUTC());
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    Future<?> holder =
        threads.submit(
            () -> {
              try (Connection conn = storeSource.getConnection()) {
                return SqliteTransactions.inTransaction(
                    conn,
                    "holder",
                    c -> {
                      entered.countDown();
                      awaitQuietly(release);
                      return null;
                    });
              }
            });
    assertThat(entered.await(30, TimeUnit.SECONDS)).isTrue();

    var save =
        new Writer(
            "snapshot-save",
            () -> store.saveSnapshot(stream, new Version(1), new ContractState("s")));
    save.awaitQueuedOnTheMutex();
    var purge = new Writer("snapshot-purge", () -> purger.purge(SubjectId.of("subject")));
    purge.awaitQueuedOnTheMutex();

    release.countDown();
    holder.get(30, TimeUnit.SECONDS);
    save.join();
    purge.join();
    assertThat(save.failure.get()).isNull();
    assertThat(purge.failure.get()).isNull();
  }
}
