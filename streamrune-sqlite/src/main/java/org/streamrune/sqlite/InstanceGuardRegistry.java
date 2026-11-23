package org.streamrune.sqlite;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.streamrune.core.types.LogSanitizer;

/**
 * The JVM-wide registry of instance guards: one operating-system lock per SQLite database file,
 * shared by every {@link SqliteDatabase} on that file.
 *
 * <p>The guard of a database file is an exclusive {@link ExclusiveFileLock} on {@code
 * <database-file>.lock}, next to the canonical database file. The registry keys each guard by the
 * real path of the lock file ({@link Path#toRealPath}), so every spelling of one file (relative,
 * absolute, with {@code ./}, through a symbolic link) reaches the same entry. Each entry holds
 * exactly one lock, through exactly one descriptor, and counts the leases on it: the lock is
 * released when the last lease is.
 *
 * <p>One descriptor per file is a requirement, not an optimization. A second lock on the same file
 * from this JVM would conflict with the first ({@code flock} is per open file description, and a
 * {@link java.nio.channels.FileLock} throws {@link
 * java.nio.channels.OverlappingFileLockException}), so nothing else in the framework opens the lock
 * file.
 *
 * <p>Acquiring and releasing run under one lock for the whole registry. Opening a database is a
 * startup step, so the lock costs nothing that matters, and it makes the steps between creating the
 * lock file and recording its entry atomic: two threads that open the same file at the same moment
 * always share one entry. The lock file is never deleted, so a restart finds it and locks it again.
 */
final class InstanceGuardRegistry {

  private static final ReentrantLock REGISTRY_LOCK = new ReentrantLock();

  /** The guards by the real path of their lock file; written only under {@link #REGISTRY_LOCK}. */
  private static final ConcurrentHashMap<Path, Entry> ENTRIES = new ConcurrentHashMap<>();

  private InstanceGuardRegistry() {}

  /** One held lock and the number of leases on it; mutated only under {@link #REGISTRY_LOCK}. */
  private static final class Entry {
    private final ExclusiveFileLock lock;

    /**
     * The identity of the locked lock file ({@link BasicFileAttributes#fileKey()}, the device and
     * inode on POSIX); {@code null} where the file system has none.
     */
    private final Object fileKey;

    private int references;

    private Entry(ExclusiveFileLock lock, Object fileKey) {
      this.lock = lock;
      this.fileKey = fileKey;
    }
  }

  /** The lock file as {@link #createAndResolve} found it. */
  private record LockFileState(Path key, boolean created, Object fileKey) {}

  /**
   * One {@link SqliteDatabase}'s share of a guard. {@link #release()} gives it back once; later
   * calls do nothing.
   */
  static final class Lease {
    private final Path databaseFile;
    private final Path lockFile;
    private final AtomicBoolean released = new AtomicBoolean();

    private Lease(Path databaseFile, Path lockFile) {
      this.databaseFile = databaseFile;
      this.lockFile = lockFile;
    }

    /** The canonical database file this lease guards. */
    Path databaseFile() {
      return databaseFile;
    }

    /** The real path of the lock file, the registry key. */
    Path lockFile() {
      return lockFile;
    }

    /** Gives this lease back; the last lease of a guard releases its lock. Idempotent. */
    void release() {
      if (released.compareAndSet(false, true)) {
        InstanceGuardRegistry.release(lockFile);
      }
    }
  }

  /**
   * Takes a lease on the guard of {@code databaseFile}, locking its lock file when this JVM does
   * not hold the lock yet.
   *
   * @param databaseFile the database file, in any spelling; it must exist
   * @return the lease, which {@link Lease#release()} gives back
   * @throws SqliteGuardException when another process holds the lock; when the database file cannot
   *     be resolved or has more than one hard link; when its lock file cannot be created, opened or
   *     locked; or when the lock file was deleted or replaced while this JVM holds its guard. The
   *     message names the file and the cause
   */
  static Lease acquire(Path databaseFile) {
    Path database = realPath(databaseFile);
    refuseHardLinks(database);
    Path lockFile = database.resolveSibling(database.getFileName() + ".lock");
    REGISTRY_LOCK.lock();
    try {
      LockFileState state = createAndResolve(database, lockFile);
      Entry entry = ENTRIES.get(state.key());
      if (entry == null) {
        entry = new Entry(lock(database, state.key()), state.fileKey());
        ENTRIES.put(state.key(), entry);
      } else if (state.created() || !Objects.equals(entry.fileKey, state.fileKey())) {
        throw new SqliteGuardException(
            "The lock file "
                + shown(state.key())
                + " of the SQLite database "
                + shown(database)
                + " was deleted or replaced while this JVM holds its guard, so another process can"
                + " now lock a new file of that name. The file is not locked again: stop every"
                + " instance on the database and restart one. The lock file must never be deleted"
                + " while the database is in use.");
      }
      entry.references++;
      return new Lease(database, state.key());
    } finally {
      REGISTRY_LOCK.unlock();
    }
  }

  /** The number of leases on the guard whose lock file is {@code lockFile}; 0 when none. */
  static int references(Path lockFile) {
    REGISTRY_LOCK.lock();
    try {
      Entry entry = ENTRIES.get(lockFile);
      return entry == null ? 0 : entry.references;
    } finally {
      REGISTRY_LOCK.unlock();
    }
  }

  private static void release(Path key) {
    REGISTRY_LOCK.lock();
    try {
      Entry entry = ENTRIES.get(key);
      entry.references--;
      if (entry.references == 0) {
        ENTRIES.remove(key);
        entry.lock.release();
      }
    } catch (IOException e) {
      throw new SqliteGuardException(
          "cannot release the lock file " + shown(key) + ": " + e.getMessage(), e);
    } finally {
      REGISTRY_LOCK.unlock();
    }
  }

  private static Path realPath(Path databaseFile) {
    try {
      return databaseFile.toRealPath();
    } catch (IOException e) {
      throw new SqliteGuardException(
          "cannot resolve the SQLite database file " + shown(databaseFile) + ": " + e, e);
    }
  }

  /**
   * Refuses a database file with more than one hard link on a POSIX file system. The guard is keyed
   * by path, so two names of one file would get two lock files, and two instances could each hold
   * "the" guard of the same database.
   */
  private static void refuseHardLinks(Path database) {
    if (!database.getFileSystem().supportedFileAttributeViews().contains("unix")) {
      return;
    }
    int links;
    try {
      links = (Integer) Files.getAttribute(database, "unix:nlink");
    } catch (IOException e) {
      throw new SqliteGuardException(
          "cannot read the link count of the SQLite database file " + shown(database) + ": " + e,
          e);
    }
    if (links > 1) {
      throw new SqliteGuardException(
          "The SQLite database file "
              + shown(database)
              + " has "
              + links
              + " hard links. The instance guard locks <database-file>.lock next to the name it"
              + " is opened under, so another name of the same file would get a lock file of its"
              + " own and a second instance could open the database through it. Remove the other"
              + " hard links and open the database under one name.");
    }
  }

  /**
   * Creates the lock file when it does not exist and returns its real path, whether this call
   * created it, and its identity. Runs under {@link #REGISTRY_LOCK}: creating a file opens and
   * closes a descriptor on it.
   */
  private static LockFileState createAndResolve(Path database, Path lockFile) {
    try {
      boolean created;
      try {
        Files.createFile(lockFile);
        created = true;
      } catch (FileAlreadyExistsException alreadyThere) {
        // A lock file is never deleted: one left by an earlier run, or by another process, is the
        // file to lock.
        created = false;
      }
      Path key = lockFile.toRealPath();
      Object fileKey = Files.readAttributes(key, BasicFileAttributes.class).fileKey();
      return new LockFileState(key, created, fileKey);
    } catch (IOException e) {
      throw new SqliteGuardException(
          "cannot guard the SQLite database "
              + shown(database)
              + ": its lock file "
              + shown(lockFile)
              + " cannot be created or opened ("
              + e
              + "). The directory of the database must be writable.",
          e);
    }
  }

  private static ExclusiveFileLock lock(Path database, Path lockFile) {
    ExclusiveFileLock lock;
    try {
      lock = ExclusiveFileLock.tryAcquire(lockFile);
    } catch (IOException e) {
      throw new SqliteGuardException(
          "cannot guard the SQLite database "
              + shown(database)
              + ": its lock file "
              + shown(lockFile)
              + " cannot be locked ("
              + e.getMessage()
              + ").",
          e);
    }
    if (lock == null) {
      throw new SqliteGuardException(
          "The SQLite database "
              + shown(database)
              + " is in use by another process, or by another class loader of this JVM: its lock"
              + " file "
              + shown(lockFile)
              + " is locked. A SQLite database serves exactly one application instance; stop the"
              + " other instance or give this one its own database file.");
    }
    return lock;
  }

  private static String shown(Path path) {
    return LogSanitizer.sanitizeFreeText(path.toString());
  }
}
