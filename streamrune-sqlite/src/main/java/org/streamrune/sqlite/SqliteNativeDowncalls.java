package org.streamrune.sqlite;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.util.ArrayList;
import java.util.List;
import org.streamrune.core.Internal;

/**
 * The C functions the instance guard calls through the Foreign Function and Memory API on Linux and
 * macOS ({@code FlockFileLock}): {@code open}, {@code flock}, {@code close} and {@code strerror}.
 * The guard binds them from these descriptors, and the integrations' native-image parity tests read
 * the same list ({@link #reachabilityMetadata()}) to check the {@code foreign} section of their
 * reachability metadata, so a changed descriptor fails those tests until the metadata follows.
 *
 * @hidden
 */
@Internal
public final class SqliteNativeDowncalls {

  /** The type whose reachability makes the downcalls registered: the guard itself, {@value}. */
  public static final String CONDITION_TYPE = "org.streamrune.sqlite.FlockFileLock";

  /**
   * One downcall.
   *
   * @param function the C function's name
   * @param descriptor its function descriptor
   * @param firstVariadicArg the index of its first variadic argument, or {@code -1} for none
   * @param capturesErrno whether the call captures {@code errno}
   */
  public record Downcall(
      String function, FunctionDescriptor descriptor, int firstVariadicArg, boolean capturesErrno) {

    /** The linker options the guard binds the function with. */
    Linker.Option[] options() {
      List<Linker.Option> options = new ArrayList<>();
      if (firstVariadicArg >= 0) {
        options.add(Linker.Option.firstVariadicArg(firstVariadicArg));
      }
      if (capturesErrno) {
        options.add(Linker.Option.captureCallState("errno"));
      }
      return options.toArray(Linker.Option[]::new);
    }

    /**
     * The downcall as a native-image reachability-metadata {@code foreign.downcalls} entry, in one
     * line: condition type, return type, parameter types, options as JSON, for example {@code
     * org.streamrune.sqlite.FlockFileLock jint [jint, jint] {"captureCallState":true}}.
     *
     * @return the line
     */
    public String reachabilityMetadataForm() {
      List<String> parameters = new ArrayList<>();
      for (MemoryLayout layout : descriptor.argumentLayouts()) {
        parameters.add(canonical(layout));
      }
      List<String> options = new ArrayList<>();
      if (firstVariadicArg >= 0) {
        options.add("\"firstVariadicArg\":" + firstVariadicArg);
      }
      if (capturesErrno) {
        options.add("\"captureCallState\":true");
      }
      return CONDITION_TYPE
          + " "
          + canonical(descriptor.returnLayout().orElseThrow())
          + " "
          + parameters
          + " {"
          + String.join(",", options)
          + "}";
    }
  }

  /** {@code int open(const char *path, int flags, ...)}, variadic from the mode. */
  public static final Downcall OPEN =
      new Downcall("open", FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_INT, JAVA_INT), 2, true);

  /** {@code int flock(int fd, int operation)}. */
  public static final Downcall FLOCK =
      new Downcall("flock", FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT), -1, true);

  /** {@code int close(int fd)}. */
  public static final Downcall CLOSE =
      new Downcall("close", FunctionDescriptor.of(JAVA_INT, JAVA_INT), -1, true);

  /** {@code char *strerror(int errnum)}. */
  public static final Downcall STRERROR =
      new Downcall("strerror", FunctionDescriptor.of(ADDRESS, JAVA_INT), -1, false);

  /** Every downcall of the guard, in the order the reachability metadata lists them. */
  public static final List<Downcall> GUARD = List.of(OPEN, FLOCK, CLOSE, STRERROR);

  private SqliteNativeDowncalls() {}

  /**
   * The {@code foreign.downcalls} entries the reachability metadata of every integration must hold,
   * one {@link Downcall#reachabilityMetadataForm()} line per downcall.
   *
   * @return the lines, in {@link #GUARD} order
   */
  public static List<String> reachabilityMetadata() {
    return GUARD.stream().map(Downcall::reachabilityMetadataForm).toList();
  }

  /** The canonical layout name native-image reads ({@code Linker.canonicalLayouts()}). */
  static String canonical(MemoryLayout layout) {
    if (JAVA_INT.equals(layout)) {
      return "jint";
    }
    if (ADDRESS.equals(layout)) {
      return "void*";
    }
    throw new IllegalArgumentException("no canonical layout name for " + layout);
  }
}
