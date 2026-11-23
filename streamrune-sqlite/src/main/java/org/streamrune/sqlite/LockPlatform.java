package org.streamrune.sqlite;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Locale;

/**
 * The operating systems the instance guard supports, and the one place that reads {@code os.name}:
 * each value says which {@link ExclusiveFileLock} it uses and, for {@code flock}, the native
 * constants that differ between systems.
 */
enum LockPlatform {

  /** Linux: {@code flock(2)}; the asm-generic constants of every architecture the JDK supports. */
  LINUX(new FlockFileLock.Constants(02000000, 11)),

  /** macOS: {@code flock(2)}; the Darwin constants. */
  MACOS(new FlockFileLock.Constants(0x1000000, 35)),

  /** Windows: a {@link java.nio.channels.FileLock}, which is held per handle there. */
  WINDOWS(null);

  private final FlockFileLock.Constants flock;

  LockPlatform(FlockFileLock.Constants flock) {
    this.flock = flock;
  }

  /** The platform of this JVM. */
  static LockPlatform current() {
    return of(System.getProperty("os.name", ""));
  }

  /**
   * The platform an {@code os.name} value names.
   *
   * @throws SqliteGuardException for any operating system other than Linux, macOS and Windows: its
   *     {@link java.nio.channels.FileLock} is a POSIX record lock, which another descriptor on the
   *     lock file being closed would release
   */
  static LockPlatform of(String osName) {
    String os = osName.toLowerCase(Locale.ROOT);
    if (os.startsWith("linux")) {
      return LINUX;
    }
    if (os.startsWith("mac")) {
      return MACOS;
    }
    if (os.startsWith("windows")) {
      return WINDOWS;
    }
    throw new SqliteGuardException(
        "The SQLite instance guard does not support the operating system '"
            + osName
            + "': it runs on Linux, macOS and Windows. Elsewhere the only portable file lock is a"
            + " POSIX record lock, which closing any other descriptor on the lock file releases.");
  }

  /** Whether this platform locks with {@code flock(2)}. */
  boolean usesFlock() {
    return flock != null;
  }

  /** The {@code flock} constants of this platform; {@code null} on Windows. */
  FlockFileLock.Constants flockConstants() {
    return flock;
  }

  /** Tries to lock {@code lockFile} with this platform's lock; see {@link ExclusiveFileLock}. */
  ExclusiveFileLock tryAcquire(Path lockFile) throws IOException {
    return usesFlock()
        ? FlockFileLock.tryAcquire(lockFile, flock, FlockFileLock.FILE_NAME_ENCODING)
        : ChannelFileLock.tryAcquire(lockFile);
  }
}
