package org.streamrune.sqlite;

import java.io.IOException;
import java.nio.file.Path;

/**
 * An exclusive operating-system lock on a lock file, held through one descriptor of its own.
 *
 * <p>The lock must stay held until {@link #release()}, whatever else the process does with the same
 * file. A {@link java.nio.channels.FileLock} cannot promise that on a POSIX system: it is a POSIX
 * record lock, which belongs to the process, and closing <em>any</em> descriptor the process has on
 * the file drops it, while {@link java.nio.channels.FileLock#isValid()} still answers {@code true}.
 * So on Linux and macOS the lock is {@code flock(2)} ({@link FlockFileLock}), which belongs to its
 * own open file description: other descriptors on the file can be opened and closed without effect,
 * and the operating system drops it when its descriptor closes or the process ends. On Windows a
 * {@link java.nio.channels.FileLock} is held per handle and closing another handle does not drop
 * it, so {@link ChannelFileLock} is used. Every other operating system is refused ({@link
 * LockPlatform#of(String)}): its {@link java.nio.channels.FileLock} would be the POSIX record lock.
 *
 * <p>Every process that guards a database must use the same kind of lock: on Linux a {@code flock}
 * and a POSIX record lock on the same file do not see each other.
 */
interface ExclusiveFileLock {

  /**
   * Tries to lock {@code lockFile}, which must exist, without waiting, with the lock of this
   * operating system.
   *
   * @return the held lock, or {@code null} when another holder has it
   * @throws IOException when the file cannot be opened or locked
   * @throws SqliteGuardException when the operating system is not supported, or the JVM denies the
   *     native access the lock needs
   */
  static ExclusiveFileLock tryAcquire(Path lockFile) throws IOException {
    return LockPlatform.current().tryAcquire(lockFile);
  }

  /**
   * Releases the lock and closes its descriptor. A second call does nothing.
   *
   * @throws IOException when the descriptor cannot be closed
   */
  void release() throws IOException;
}
