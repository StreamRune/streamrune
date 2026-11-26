package org.streamrune.sqlite;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.EventStoreContract.ContractEvent;
import org.streamrune.test.EventStoreContract.ContractState;

/**
 * The program {@link SqliteWriteMutexStarvationTest} runs in a second JVM, whose virtual-thread
 * scheduler has as few carriers as the test asks for: a projection batch on a virtual thread whose
 * updater parks for a while inside its write transaction, as one that makes a blocking call does,
 * while other virtual threads write to the same file through the stores.
 *
 * <p>{@code <db> <writers> <hold-millis>}: the batch's updater writes a row and sleeps {@code
 * hold-millis}; once it sleeps, {@code writers} virtual threads each make one write, by turns a
 * projection save (a {@code BEGIN IMMEDIATE} transaction), an append (the event store's own
 * transaction) and a snapshot save (one statement in autocommit). Each prints one line: {@value
 * #BATCH} or {@code WRITER <n>}, then {@value #OK} or {@value #FAILED}, then the milliseconds it
 * took. The connections' {@code busy_timeout} is {@link #BUSY_MILLIS}.
 */
public final class WriteStarvationChild {

  static final int BUSY_MILLIS = 5_000;
  static final String BATCH = "BATCH";
  static final String WRITER = "WRITER";
  static final String OK = "OK";
  static final String FAILED = "FAILED";
  static final String DONE = "DONE";

  private static final ProjectionName ORDERS = ProjectionName.of("orders");
  private static final ProjectionName AUDIT = ProjectionName.of("audit_trail");

  private WriteStarvationChild() {}

  public static void main(String[] args) throws Exception {
    Path db = Path.of(args[0]);
    int writers = Integer.parseInt(args[1]);
    long holdMillis = Long.parseLong(args[2]);
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:sqlite:file:" + db + "?" + TestDatabases.REQUIRED_SETTINGS);
    config.setMaximumPoolSize(writers + 4);
    try (HikariDataSource pool = new HikariDataSource(config);
        SqliteDatabase database = SqliteDatabase.open(pool)) {
      EventStore store =
          new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry())
              .create();
      database.migrate(SqliteSchemaSeries.READ_MODEL);
      var checkpoints = new SqliteReadModelCheckpoints(database);
      var repo =
          new SqliteProjectionRepository(
              database,
              SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP),
              checkpoints);
      // Every table exists and every connection the writers need is open before the batch starts.
      repo.save(AUDIT, "warm", Map.of("n", 0));
      repo.prepareReadModel(ORDERS);
      for (int i = 0; i < writers; i++) {
        store.append(stream(i), events(stream(i), 1), Version.initial());
      }

      var inUpdater = new CountDownLatch(1);
      Thread batch =
          Thread.ofVirtual()
              .name("projection-orders")
              .start(
                  () ->
                      timed(
                          BATCH,
                          () ->
                              repo.executeAtomically(
                                  ORDERS,
                                  List.of(),
                                  GlobalOffset.of(1),
                                  0L,
                                  tx -> {
                                    tx.save(ORDERS, "o-1", Map.of("status", "PLACED"));
                                    inUpdater.countDown();
                                    sleep(holdMillis);
                                  },
                                  checkpoints)));
      inUpdater.await();
      List<Thread> threads = new ArrayList<>();
      for (int i = 0; i < writers; i++) {
        int n = i;
        Runnable write =
            switch (n % 3) {
              case 0 -> () -> repo.save(AUDIT, "a-" + n, Map.of("n", n));
              case 1 -> () -> store.append(stream(n), events(stream(n), 1), new Version(1));
              default ->
                  () -> store.saveSnapshot(stream(n), new Version(1), new ContractState("s"));
            };
        threads.add(
            Thread.ofVirtual().name("writer-" + n).start(() -> timed(WRITER + " " + n, write)));
      }
      batch.join();
      for (Thread thread : threads) {
        thread.join();
      }
    }
    print(DONE);
  }

  private static void timed(String who, Runnable work) {
    long start = System.nanoTime();
    try {
      work.run();
      print(who + " " + OK + " " + (System.nanoTime() - start) / 1_000_000);
    } catch (RuntimeException e) {
      print(who + " " + FAILED + " " + (System.nanoTime() - start) / 1_000_000 + " " + e);
    }
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static StreamId stream(int n) {
    return StreamId.of(AggregateType.of("order"), AggregateId.of("w" + n));
  }

  private static List<EventEnvelope> events(StreamId stream, int count) {
    List<EventEnvelope> events = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      events.add(
          new EventEnvelope(
              GlobalOffset.initial(),
              stream,
              Version.initial(),
              EventStoreContract.EVENT_TYPE,
              new ContractEvent("e" + i),
              new EventMetadata(
                  IdGenerator.generateEventId(),
                  IdGenerator.generateCommandId(),
                  null,
                  CorrelationId.of(UUID.randomUUID().toString()),
                  null,
                  null,
                  Instant.now())));
    }
    return events;
  }

  private static void print(String line) {
    System.out.println(line);
    System.out.flush();
  }
}
