package org.streamrune.runtime.jdbc;

import java.util.function.Predicate;
import org.streamrune.core.Internal;

/**
 * The bounded walk of a {@link Throwable}'s {@link Throwable#getCause() cause} chain that the
 * stores use to classify a failure. The throwable itself is the first link; at most {@link
 * #MAX_DEPTH} links are inspected, so a self-referential or cyclic chain terminates instead of
 * spinning forever.
 *
 * @hidden
 */
@Internal
public final class Causes {

  /** The most links of a cause chain a walk inspects, the throwable itself included. */
  public static final int MAX_DEPTH = 50;

  private Causes() {}

  /**
   * The first link of {@code t}'s cause chain that is a {@code type}.
   *
   * @param t the throwable to walk, or {@code null}
   * @param type the type to look for
   * @param <T> the type to look for
   * @return the first match among the first {@link #MAX_DEPTH} links, or {@code null}
   */
  public static <T extends Throwable> T find(Throwable t, Class<T> type) {
    Throwable c = t;
    for (int depth = 0; c != null && depth < MAX_DEPTH; depth++, c = c.getCause()) {
      if (type.isInstance(c)) {
        return type.cast(c);
      }
    }
    return null;
  }

  /**
   * Whether a link of {@code t}'s cause chain satisfies {@code test}; the same walk as {@link
   * #find}.
   *
   * @param t the throwable to walk, or {@code null}
   * @param test the test applied to each link
   * @return {@code true} if one of the first {@link #MAX_DEPTH} links matches
   */
  public static boolean anyMatch(Throwable t, Predicate<? super Throwable> test) {
    Throwable c = t;
    for (int depth = 0; c != null && depth < MAX_DEPTH; depth++, c = c.getCause()) {
      if (test.test(c)) {
        return true;
      }
    }
    return false;
  }
}
