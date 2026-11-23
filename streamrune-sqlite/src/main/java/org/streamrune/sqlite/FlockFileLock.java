package org.streamrune.sqlite;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
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
 * <p>The native calls need native access for this code ({@code --enable-native-access=ALL-UNNAMED}
 * on the class path); the sqlite-jdbc driver needs it for its native library as well. The calls are
 * bound on first use: when the JVM denies native access the lock fails with a {@link
 * SqliteGuardException} that names the option, and nothing falls back to a weaker lock.
 */
final class FlockFileLock implements ExclusiveFileLock {

  /** The constants that differ between Linux and macOS, chosen by {@link LockPlatform}. */
  record Constants(int oCloexec, int eWouldBlock) {}

  /** The encoding the JDK uses for file names, so that the native call opens the same file. */
  static final Charset FILE_NAME_ENCODING =
      Charset.forName(System.getProperty("sun.jnu.encoding", "UTF-8"));

  private static final int O_RDONLY = 0;
  private static final int LOCK_EX = 2;
  private static final int LOCK_NB = 4;
  private static final int EINTR = 4;

  private static final StructLayout CALL_STATE = Linker.Option.captureStateLayout();
  private static final VarHandle ERRNO =
      CALL_STATE.varHandle(MemoryLayout.PathElement.groupElement("errno"));

  /** The four C functions, bound on first use; see {@link #natives()}. */
  private record Natives(
      MethodHandle open, MethodHandle flock, MethodHandle close, MethodHandle strerror) {}

  /** Written once, under the class lock, by {@link #natives()}. */
  private static Natives bound;

  private final Path lockFile;
  private final int descriptor;
  private boolean released;

  private FlockFileLock(Path lockFile, int descriptor) {
    this.lockFile = lockFile;
    this.descriptor = descriptor;
  }

  /**
   * Opens {@code lockFile}, which must exist, and tries to {@code flock} it without waiting.
   *
   * @param constants the native constants of this operating system
   * @param fileNameEncoding the encoding of file names on this system
   * @return the held lock, or {@code null} when another open file description holds it
   * @throws IOException when the file cannot be opened or locked; the message names the file and
   *     the error
   * @throws SqliteGuardException when the JVM denies native access
   */
  static FlockFileLock tryAcquire(Path lockFile, Constants constants, Charset fileNameEncoding)
      throws IOException {
    Natives natives = natives();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment state = arena.allocate(CALL_STATE);
      MemorySegment path = cString(arena, lockFile.toString(), fileNameEncoding);
      int descriptor;
      do {
        descriptor =
            (int) natives.open().invokeExact(state, path, O_RDONLY | constants.oCloexec(), 0);
      } while (descriptor < 0 && errno(state) == EINTR);
      if (descriptor < 0) {
        throw failure(natives, "cannot open the lock file " + lockFile, errno(state));
      }
      int result;
      do {
        result = (int) natives.flock().invokeExact(state, descriptor, LOCK_EX | LOCK_NB);
      } while (result != 0 && errno(state) == EINTR);
      if (result == 0) {
        return new FlockFileLock(lockFile, descriptor);
      }
      int error = errno(state);
      int ignored = (int) natives.close().invokeExact(state, descriptor);
      if (error == constants.eWouldBlock()) {
        return null;
      }
      throw failure(natives, "cannot lock the lock file " + lockFile, error);
    } catch (IOException | RuntimeException | Error e) {
      throw e;
    } catch (Throwable e) {
      throw new IllegalStateException("native call failed for the lock file " + lockFile, e);
    }
  }

  /**
   * Closes the descriptor, which releases the lock. A second call does nothing: the descriptor
   * number may already belong to another file of this process.
   */
  @Override
  public synchronized void release() throws IOException {
    if (released) {
      return;
    }
    released = true;
    Natives natives = natives();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment state = arena.allocate(CALL_STATE);
      if ((int) natives.close().invokeExact(state, descriptor) != 0) {
        throw failure(natives, "cannot close the lock file " + lockFile, errno(state));
      }
    } catch (IOException | RuntimeException | Error e) {
      throw e;
    } catch (Throwable e) {
      throw new IllegalStateException("native call failed for the lock file " + lockFile, e);
    }
  }

  /**
   * {@code value} as a NUL-terminated C string in {@code charset}. Encoded here rather than through
   * {@link Arena#allocateFrom(String, Charset)}, which accepts only the standard charsets, so that
   * a system whose file names are, for example, ISO-8859-2 or EUC-JP can lock its files.
   */
  static MemorySegment cString(Arena arena, String value, Charset charset) {
    byte[] bytes = value.getBytes(charset);
    MemorySegment segment = arena.allocate(bytes.length + 1L);
    MemorySegment.copy(bytes, 0, segment, JAVA_BYTE, 0, bytes.length);
    return segment;
  }

  /**
   * Binds the C functions on first use. Binding is a restricted operation: a JVM that denies native
   * access to this code throws {@link IllegalCallerException}, and a JVM without a native linker
   * for this system throws {@link UnsupportedOperationException}. Both become a {@link
   * SqliteGuardException}; nothing is cached after a failure, and no weaker lock replaces this one.
   */
  private static synchronized Natives natives() {
    if (bound == null) {
      try {
        Linker linker = Linker.nativeLinker();
        bound =
            new Natives(
                downcall(
                    linker,
                    "open",
                    FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT),
                    Linker.Option.firstVariadicArg(2),
                    Linker.Option.captureCallState("errno")),
                downcall(
                    linker,
                    "flock",
                    FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT),
                    Linker.Option.captureCallState("errno")),
                downcall(
                    linker,
                    "close",
                    FunctionDescriptor.of(JAVA_INT, JAVA_INT),
                    Linker.Option.captureCallState("errno")),
                downcall(linker, "strerror", FunctionDescriptor.of(ADDRESS, JAVA_INT)));
      } catch (IllegalCallerException | UnsupportedOperationException e) {
        throw new SqliteGuardException(
            "The SQLite instance guard locks its lock file with flock(2) through the Foreign"
                + " Function and Memory API, and this JVM does not allow it ("
                + e.getMessage()
                + "). Start the JVM with --enable-native-access=ALL-UNNAMED (or the name of the"
                + " module that holds StreamRune); the sqlite-jdbc driver needs native access as"
                + " well.",
            e);
      }
    }
    return bound;
  }

  private static int errno(MemorySegment state) {
    return (int) ERRNO.get(state, 0L);
  }

  private static IOException failure(Natives natives, String what, int errno) throws Throwable {
    MemorySegment message = (MemorySegment) natives.strerror().invokeExact(errno);
    return new IOException(
        what
            + ": "
            + message.reinterpret(Integer.MAX_VALUE).getString(0)
            + " (errno "
            + errno
            + ")");
  }

  private static MethodHandle downcall(
      Linker linker, String name, FunctionDescriptor descriptor, Linker.Option... options) {
    MemorySegment symbol =
        linker
            .defaultLookup()
            .find(name)
            .orElseThrow(() -> new IllegalStateException("the C library has no " + name));
    return linker.downcallHandle(symbol, descriptor, options);
  }
}
