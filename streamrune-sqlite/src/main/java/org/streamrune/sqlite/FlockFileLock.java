package org.streamrune.sqlite;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.charset.Charset;
import java.nio.file.Path;

/**
 * An {@link ExclusiveFileLock} held as {@code flock(fd, LOCK_EX | LOCK_NB)} on a descriptor this
 * class opens and closes itself, through the Foreign Function and Memory API: the lock on Linux and
 * macOS.
 *
 * <p>A {@code flock} belongs to the open file description, so no other descriptor on the file, in
 * this process or another, can drop it; only closing this descriptor, or the end of the process,
 * releases it. The descriptor is opened with {@code O_CLOEXEC}, so a program the JVM starts does
 * not inherit it and cannot keep the lock alive after this process has ended.
 *
 * <p>The JVM reports the first use of the restricted {@link Linker} methods with a warning unless
 * native access is enabled for this module ({@code --enable-native-access=ALL-UNNAMED} on the class
 * path); the sqlite-jdbc driver needs the same option for its native library.
 */
final class FlockFileLock implements ExclusiveFileLock {

  private static final int O_RDONLY = 0;
  private static final int LOCK_EX = 2;
  private static final int LOCK_NB = 4;
  private static final int EINTR = 4;

  /** The constants that differ between Linux (every architecture the JDK supports) and macOS. */
  private record Platform(int oCloexec, int eWouldBlock) {}

  private static final Platform PLATFORM =
      System.getProperty("os.name", "").toLowerCase(java.util.Locale.ROOT).startsWith("mac")
          ? new Platform(0x1000000, 35)
          : new Platform(02000000, 11);

  /** The encoding the JDK uses for file names, so that the native call opens the same file. */
  private static final Charset FILE_NAME_ENCODING =
      Charset.forName(System.getProperty("sun.jnu.encoding", "UTF-8"));

  private static final Linker LINKER = Linker.nativeLinker();
  private static final StructLayout CALL_STATE = Linker.Option.captureStateLayout();
  private static final VarHandle ERRNO =
      CALL_STATE.varHandle(MemoryLayout.PathElement.groupElement("errno"));
  private static final MethodHandle OPEN =
      downcall(
          "open",
          FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT),
          Linker.Option.firstVariadicArg(2),
          Linker.Option.captureCallState("errno"));
  private static final MethodHandle FLOCK =
      downcall(
          "flock",
          FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT),
          Linker.Option.captureCallState("errno"));
  private static final MethodHandle CLOSE =
      downcall(
          "close",
          FunctionDescriptor.of(JAVA_INT, JAVA_INT),
          Linker.Option.captureCallState("errno"));
  private static final MethodHandle STRERROR =
      downcall("strerror", FunctionDescriptor.of(ADDRESS, JAVA_INT));

  private final Path lockFile;
  private final int descriptor;

  private FlockFileLock(Path lockFile, int descriptor) {
    this.lockFile = lockFile;
    this.descriptor = descriptor;
  }

  /**
   * Opens {@code lockFile}, which must exist, and tries to {@code flock} it without waiting.
   *
   * @return the held lock, or {@code null} when another open file description holds it
   * @throws IOException when the file cannot be opened or locked; the message names the file and
   *     the error
   */
  static FlockFileLock tryAcquire(Path lockFile) throws IOException {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment state = arena.allocate(CALL_STATE);
      MemorySegment path = arena.allocateFrom(lockFile.toString(), FILE_NAME_ENCODING);
      int descriptor;
      do {
        descriptor = (int) OPEN.invokeExact(state, path, O_RDONLY | PLATFORM.oCloexec(), 0);
      } while (descriptor < 0 && errno(state) == EINTR);
      if (descriptor < 0) {
        throw failure("cannot open the lock file " + lockFile, errno(state));
      }
      int result;
      do {
        result = (int) FLOCK.invokeExact(state, descriptor, LOCK_EX | LOCK_NB);
      } while (result != 0 && errno(state) == EINTR);
      if (result == 0) {
        return new FlockFileLock(lockFile, descriptor);
      }
      int error = errno(state);
      int ignored = (int) CLOSE.invokeExact(state, descriptor);
      if (error == PLATFORM.eWouldBlock()) {
        return null;
      }
      throw failure("cannot lock the lock file " + lockFile, error);
    } catch (IOException | RuntimeException | Error e) {
      throw e;
    } catch (Throwable e) {
      throw new IllegalStateException("native call failed for the lock file " + lockFile, e);
    }
  }

  @Override
  public void release() throws IOException {
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment state = arena.allocate(CALL_STATE);
      if ((int) CLOSE.invokeExact(state, descriptor) != 0) {
        throw failure("cannot close the lock file " + lockFile, errno(state));
      }
    } catch (IOException | RuntimeException | Error e) {
      throw e;
    } catch (Throwable e) {
      throw new IllegalStateException("native call failed for the lock file " + lockFile, e);
    }
  }

  private static int errno(MemorySegment state) {
    return (int) ERRNO.get(state, 0L);
  }

  private static IOException failure(String what, int errno) throws Throwable {
    MemorySegment message = (MemorySegment) STRERROR.invokeExact(errno);
    return new IOException(
        what
            + ": "
            + message.reinterpret(Integer.MAX_VALUE).getString(0)
            + " (errno "
            + errno
            + ")");
  }

  private static MethodHandle downcall(
      String name, FunctionDescriptor descriptor, Linker.Option... options) {
    MemorySegment symbol =
        LINKER
            .defaultLookup()
            .find(name)
            .orElseThrow(() -> new IllegalStateException("the C library has no " + name));
    return LINKER.downcallHandle(symbol, descriptor, options);
  }
}
