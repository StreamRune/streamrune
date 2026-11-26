package org.streamrune.crypto.sqlite;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.sqlite.SqliteDatabase;
import org.streamrune.sqlite.SqliteProjectionRepository;
import org.streamrune.sqlite.SqliteReadModelCheckpoints;
import org.streamrune.sqlite.SqliteSchemaSeries;

/**
 * The lock order of a cached SQLite engine: an erasure through {@link CachedCryptoEngine} holds the
 * subject's stripe and waits for the file's write lock; a projection batch holds that lock and
 * cache-misses a decrypt of a subject of the same stripe. The decrypt waits a bounded time for the
 * stripe and then decrypts uncached, so the batch commits, the erasure takes the lock and succeeds,
 * and neither waits for {@code busy_timeout}.
 */
class CachedSqliteCryptoEngineLockOrderTest {

  private static final ProjectionName P = ProjectionName.of("lock_order");

  @TempDir Path dir;

  private SqliteDatabase database;

  @AfterEach
  void close() {
    database.close();
  }

  /** A subject of the same cache stripe as {@code subject}, other than it. */
  private static SubjectId sameStripeAs(SubjectId subject) {
    int stripe = Math.floorMod(subject.hashCode(), 64);
    for (int i = 0; ; i++) {
      SubjectId candidate = SubjectId.of("neighbour-" + i);
      if (Math.floorMod(candidate.hashCode(), 64) == stripe && !candidate.equals(subject)) {
        return candidate;
      }
    }
  }

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void anErasureOverlappingABatchsDecryptOfTheSameStripeSucceeds() throws Exception {
    database = SqliteFiles.migrated(SqliteFiles.dataSource(dir.resolve("order.db")));
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    var checkpoints = new SqliteReadModelCheckpoints(database);
    var repo =
        new SqliteProjectionRepository(
            database,
            SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP),
            checkpoints);
    CryptoEngine cached =
        CachedCryptoEngine.builder()
            .delegate(SqliteCryptoEngine.builder().database(database).build())
            .build();
    SubjectId erased = SubjectId.of("erased");
    SubjectId read = sameStripeAs(erased);
    cached.encrypt(erased, "gone".getBytes(UTF_8));
    byte[] value = cached.encrypt(read, "kept".getBytes(UTF_8));
    var erasureFailure = new AtomicReference<Throwable>();
    var erasure = new AtomicReference<Thread>();
    var decrypted = new AtomicReference<byte[]>();
    var decryptMillis = new AtomicLong();
    long start = System.nanoTime();

    repo.executeAtomically(
        P,
        List.of(),
        GlobalOffset.of(1),
        0L,
        tx -> {
          erasure.set(
              Thread.ofPlatform()
                  .start(
                      () -> {
                        try {
                          cached.deleteKey(erased);
                        } catch (Throwable t) {
                          erasureFailure.set(t);
                        }
                      }));
          // Let the erasure take its stripe and start waiting for the write lock this batch holds.
          sleep(300);
          long decryptStart = System.nanoTime();
          decrypted.set(cached.decrypt(read, value));
          decryptMillis.set((System.nanoTime() - decryptStart) / 1_000_000);
        },
        checkpoints);
    long batchMillis = (System.nanoTime() - start) / 1_000_000;
    erasure.get().join();

    assertThat(decrypted.get()).isEqualTo("kept".getBytes(UTF_8));
    assertThat(erasureFailure.get()).as("the erasure succeeds").isNull();
    assertThat(cached.isKeyAvailable(erased)).isFalse();
    assertThat(decryptMillis.get()).as("the decrypt waited only its bound").isLessThan(2_000);
    assertThat(batchMillis).as("the batch never waited for busy_timeout").isLessThan(4_000);
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
