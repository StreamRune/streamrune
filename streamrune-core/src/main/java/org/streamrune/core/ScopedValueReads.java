package org.streamrune.core;

import java.util.Objects;

/**
 * Reads a {@link ScopedValue} from the bindings of the current thread, every time.
 *
 * <p>{@link ScopedValue#get()} and {@link ScopedValue#isBound()} answer from a small per-thread
 * cache of recently read values and walk the thread's bindings only when the cache has no entry;
 * {@link ScopedValue#orElse} walks the bindings on every call. On GraalVM CE 25.0.1 with its
 * default Graal JIT, a compiled read of that cache on a virtual thread that has been unmounted and
 * remounted answers with entries that are not the thread's bindings: a value that is bound reads as
 * unbound, and one that is not bound reads as bound. The same reads compiled by C2, or interpreted,
 * answer from the bindings.
 *
 * <p>The framework's own scoped values — {@link StreamRuneContext#CURRENT}, {@link
 * StreamRuneContext#CAUSE}, {@link StreamRuneContext#SAGA_OWNED}, {@link
 * StreamRuneContext#SAGA_FORWARD} and the command bus's own — decide authorization, the provenance
 * of every stored event and the refusal of a reserved correlation id. The framework reads each of
 * them here, never through {@code get()} or {@code isBound()}. Application code reads the request
 * context through {@link StreamRuneContext#capture()} and {@link StreamRuneContext#cause()}, which
 * read here too.
 *
 * <p>A walk of the bindings costs a few comparisons per binding scope on the thread; the framework
 * nests a handful.
 */
public final class ScopedValueReads {

  /** What {@link ScopedValue#orElse} answers for a value that is not bound; never bound itself. */
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
