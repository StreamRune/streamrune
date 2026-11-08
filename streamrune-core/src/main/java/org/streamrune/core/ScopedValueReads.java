package org.streamrune.core;

import java.util.Objects;

/**
 * The one place the framework reads a {@link ScopedValue}.
 *
 * <p>The framework reads its own scoped values — {@link StreamRuneContext#CURRENT}, {@link
 * StreamRuneContext#CAUSE}, {@link StreamRuneContext#SAGA_OWNED}, {@link
 * StreamRuneContext#SAGA_FORWARD} and the command bus's own — here and nowhere else, which an
 * architecture test of the build holds it to, so every read has one implementation. Application
 * code reads the request context through {@link StreamRuneContext#capture()} and {@link
 * StreamRuneContext#cause()}, which read here too.
 *
 * <p>On JDK 25 the Graal JIT is not reliable with StreamRune, and the command bus does not start on
 * a JVM that runs it unless the JVMCI compiler is off ({@code
 * org.streamrune.runtime.GraalJitGuard}).
 */
public final class ScopedValueReads {

  /** The answer of a read of a value that is not bound; never bound itself. */
  private static final Object NOT_BOUND = new Object();

  private ScopedValueReads() {}

  /**
   * Whether {@code key} is bound on the current thread, to any value, {@code null} included.
   *
   * @param key the scoped value (required)
   * @return {@code true} when a binding of {@code key} is in scope
   * @throws NullPointerException if {@code key} is {@code null}
   */
  public static boolean isBound(ScopedValue<?> key) {
    return read(key) != NOT_BOUND;
  }

  /**
   * The value {@code key} is bound to on the current thread.
   *
   * @param key the scoped value (required)
   * @param <T> the type of the value
   * @return the bound value; {@code null} when {@code key} is not bound, or is bound to {@code
   *     null}
   * @throws NullPointerException if {@code key} is {@code null}
   */
  @SuppressWarnings("unchecked")
  public static <T> T valueOrNull(ScopedValue<T> key) {
    Object value = read(key);
    return value == NOT_BOUND ? null : (T) value;
  }

  @SuppressWarnings("unchecked")
  private static Object read(ScopedValue<?> key) {
    Objects.requireNonNull(key, "key is required");
    return ((ScopedValue<Object>) key).orElse(NOT_BOUND);
  }
}
