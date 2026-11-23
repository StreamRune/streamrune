package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InstanceGuardRegistryTest {

  @TempDir Path dir;

  @Test
  void acquireCountsReferencesAndEachLeaseReleasesOnce() throws IOException {
    Path db = Files.createFile(dir.resolve("db.sqlite"));
    Path lockFile = SqliteDatabaseTest.lockFileOf(db);

    InstanceGuardRegistry.Lease first = InstanceGuardRegistry.acquire(db);
    InstanceGuardRegistry.Lease second = InstanceGuardRegistry.acquire(db);

    assertThat(first.databaseFile()).isEqualTo(db.toRealPath());
    assertThat(first.lockFile()).isEqualTo(lockFile).isEqualTo(second.lockFile());
    assertThat(InstanceGuardRegistry.references(lockFile)).isEqualTo(2);

    first.release();
    first.release();
    assertThat(InstanceGuardRegistry.references(lockFile)).isEqualTo(1);

    second.release();
    assertThat(InstanceGuardRegistry.references(lockFile)).isZero();
  }

  @Test
  void lockIsHeldUntilTheLastLeaseIsReleased() throws IOException {
    assumeThat(ExclusiveFileLock.usesFlock()).as("flock conflicts inside one JVM").isTrue();
    Path db = Files.createFile(dir.resolve("db.sqlite"));
    Path lockFile = SqliteDatabaseTest.lockFileOf(db);
    InstanceGuardRegistry.Lease first = InstanceGuardRegistry.acquire(db);
    InstanceGuardRegistry.Lease second = InstanceGuardRegistry.acquire(db);

    first.release();
    assertThat(ExclusiveFileLock.tryAcquire(lockFile)).isNull();

    second.release();
    ExclusiveFileLock independent = ExclusiveFileLock.tryAcquire(lockFile);
    assertThat(independent).isNotNull();
    independent.release();
  }

  @Test
  void lockSurvivesOtherDescriptorsOnTheLockFileBeingClosed() throws IOException {
    assumeThat(ExclusiveFileLock.usesFlock()).as("flock conflicts inside one JVM").isTrue();
    Path db = Files.createFile(dir.resolve("db.sqlite"));
    Path lockFile = SqliteDatabaseTest.lockFileOf(db);
    InstanceGuardRegistry.Lease lease = InstanceGuardRegistry.acquire(db);
    try {
      new FileInputStream(lockFile.toFile()).close();
      try (FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.WRITE)) {
        FileLock fcntl = channel.tryLock();
        if (fcntl != null) {
          fcntl.release();
        }
      }

      assertThat(ExclusiveFileLock.tryAcquire(lockFile)).isNull();
    } finally {
      lease.release();
    }
  }

  @Test
  void concurrentAcquiresShareOneEntry() throws Exception {
    Path db = Files.createFile(dir.resolve("db.sqlite"));
    int threads = 32;
    CyclicBarrier barrier = new CyclicBarrier(threads);
    List<InstanceGuardRegistry.Lease> leases = new ArrayList<>();
    try (ExecutorService pool = Executors.newFixedThreadPool(threads)) {
      List<Future<InstanceGuardRegistry.Lease>> acquired = new ArrayList<>();
      for (int i = 0; i < threads; i++) {
        acquired.add(
            pool.submit(
                () -> {
                  barrier.await(ForkedJvm.TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                  return InstanceGuardRegistry.acquire(db);
                }));
      }
      for (Future<InstanceGuardRegistry.Lease> lease : acquired) {
        leases.add(lease.get(ForkedJvm.TIMEOUT.toSeconds(), TimeUnit.SECONDS));
      }

      assertThat(InstanceGuardRegistry.references(SqliteDatabaseTest.lockFileOf(db)))
          .isEqualTo(threads);
    } finally {
      leases.forEach(InstanceGuardRegistry.Lease::release);
    }
    assertThat(InstanceGuardRegistry.references(SqliteDatabaseTest.lockFileOf(db))).isZero();
  }

  @Test
  void spellingsOfOnePathShareOneEntry() throws IOException {
    Path db = Files.createFile(dir.resolve("db.sqlite"));
    Path link = Files.createSymbolicLink(dir.resolve("link.sqlite"), db);
    Path relative = Path.of("").toAbsolutePath().relativize(db.toAbsolutePath());
    List<Path> spellings =
        List.of(
            db,
            link,
            relative,
            Path.of(".").resolve(relative),
            dir.resolve(".").resolve("db.sqlite"));
    List<InstanceGuardRegistry.Lease> leases = new ArrayList<>();
    try {
      for (Path spelling : spellings) {
        leases.add(InstanceGuardRegistry.acquire(spelling));
      }

      assertThat(leases)
          .extracting(InstanceGuardRegistry.Lease::lockFile)
          .containsOnly(SqliteDatabaseTest.lockFileOf(db));
      assertThat(InstanceGuardRegistry.references(SqliteDatabaseTest.lockFileOf(db)))
          .isEqualTo(spellings.size());
    } finally {
      leases.forEach(InstanceGuardRegistry.Lease::release);
    }
  }

  @Test
  void missingDatabaseFileFailsNamingTheFile() {
    Path db = dir.resolve("missing.sqlite");

    assertThatThrownBy(() -> InstanceGuardRegistry.acquire(db))
        .isInstanceOf(SqliteGuardException.class)
        .hasMessageContaining(db.toString())
        .hasCauseInstanceOf(NoSuchFileException.class);
  }

  @Test
  void lockFileHeldByAnotherProcessIsRefusedNamingBothFiles() throws Exception {
    Path db = dir.resolve("db.sqlite");
    try (ForkedJvm holder = ForkedJvm.start("hold", db)) {
      holder.awaitLineStartingWith(GuardChild.READY);

      assertThatThrownBy(() -> InstanceGuardRegistry.acquire(db))
          .isInstanceOf(SqliteGuardException.class)
          .hasMessageContaining(db.toRealPath().toString())
          .hasMessageContaining(SqliteDatabaseTest.lockFileOf(db).toString())
          .hasMessageContaining("another process");
      assertThat(InstanceGuardRegistry.references(SqliteDatabaseTest.lockFileOf(db))).isZero();
    }
  }

  @Test
  void flockOnAMissingFileFailsNamingIt() {
    assumeThat(ExclusiveFileLock.usesFlock()).isTrue();
    Path missing = dir.resolve("absent.lock");

    assertThatThrownBy(() -> FlockFileLock.tryAcquire(missing))
        .isInstanceOf(IOException.class)
        .hasMessageContaining(missing.toString())
        .hasMessageContaining("errno");
  }

  @Test
  void channelLockAcquiresReleasesAndRefusesAnOverlap() throws IOException {
    Path lockFile = Files.createFile(dir.resolve("channel.lock"));

    ExclusiveFileLock lock = ChannelFileLock.tryAcquire(lockFile);
    assertThat(lock).isNotNull();
    try {
      assertThatThrownBy(() -> ChannelFileLock.tryAcquire(lockFile))
          .isInstanceOf(IOException.class)
          .hasMessageContaining(lockFile.toString());
    } finally {
      lock.release();
    }

    ExclusiveFileLock again = ChannelFileLock.tryAcquire(lockFile);
    assertThat(again).isNotNull();
    again.release();
  }

  @Test
  void channelLockOnAMissingFileFails() {
    assertThatThrownBy(() -> ChannelFileLock.tryAcquire(dir.resolve("absent.lock")))
        .isInstanceOf(NoSuchFileException.class);
  }
}
