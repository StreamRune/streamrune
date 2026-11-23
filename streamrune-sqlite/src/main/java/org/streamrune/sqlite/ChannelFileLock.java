package org.streamrune.sqlite;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * An {@link ExclusiveFileLock} held as a {@link FileLock} on a {@link FileChannel} of its own: the
 * lock on platforms other than Linux and macOS, where such a lock belongs to its handle.
 */
final class ChannelFileLock implements ExclusiveFileLock {

  private final FileChannel channel;

  private ChannelFileLock(FileChannel channel) {
    this.channel = channel;
  }

  /**
   * Tries to lock {@code lockFile}, which must exist, without waiting.
   *
   * @return the held lock, or {@code null} when another process holds it
   * @throws IOException when the file cannot be opened or locked, or when other code in this JVM
   *     already locks it
   */
  static ChannelFileLock tryAcquire(Path lockFile) throws IOException {
    FileChannel channel = FileChannel.open(lockFile, StandardOpenOption.WRITE);
    try {
      FileLock lock = channel.tryLock();
      if (lock == null) {
        channel.close();
        return null;
      }
      return new ChannelFileLock(channel);
    } catch (OverlappingFileLockException e) {
      channel.close();
      throw new IOException(
          "the lock file " + lockFile + " is already locked by other code in this JVM", e);
    } catch (IOException e) {
      channel.close();
      throw e;
    }
  }

  @Override
  public void release() throws IOException {
    channel.close();
  }
}
