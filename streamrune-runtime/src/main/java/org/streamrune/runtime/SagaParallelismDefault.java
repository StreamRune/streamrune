package org.streamrune.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.streamrune.core.Internal;
import org.streamrune.core.types.LogSanitizer;

/**
 * Supplies a deployment's saga parallelism settings to application-constructed {@link SagaRunner}
 * beans, as {@link SagaForwardFenceDefault} does for the forward fence. The integrations call it at
 * startup; precedence is the subscription property, then the builder, then the global property,
 * then the default of 4.
 *
 * @hidden
 */
@Internal
public final class SagaParallelismDefault {

  private SagaParallelismDefault() {}

  /**
   * Applies {@code settings} to every runner.
   *
   * @param runners the discovered saga runners
   * @param settings the configured settings
   * @hidden
   */
  public static void apply(
      Collection<? extends SagaRunner<?>> runners, SagaParallelismSettings settings) {
    for (SagaRunner<?> runner : runners) {
      runner.applyParallelismSettings(settings);
    }
  }

  /**
   * Refuses a configured subscription name that no runner is bound to.
   *
   * @throws IllegalStateException naming the unknown and the known names
   * @hidden
   */
  public static void requireKnownSubscriptions(
      Collection<? extends SagaRunner<?>> runners, SagaParallelismSettings settings) {
    Set<String> known = new TreeSet<>();
    for (SagaRunner<?> runner : runners) {
      runner.boundSubscriptionNames().forEach(name -> known.add(name.value()));
    }
    Set<String> unknown = new TreeSet<>(settings.bySubscription().keySet());
    unknown.removeAll(known);
    if (!unknown.isEmpty()) {
      throw new IllegalStateException(
          "streamrune.saga.subscriptions.<name>.parallelism names a subscription no saga runner is"
              + " bound to: "
              + sanitized(unknown)
              + ". Subscriptions bound to a saga runner: "
              + sanitized(known)
              + ". A saga's subscription is bound when it is built with .sagaRunner(runner) before"
              + " startup ends.");
    }
  }

  /**
   * One startup line per runner. A runner bound to k subscriptions can have k pages in flight, one
   * per subscription, so its steps hold up to {@code parallelism x k x connectionsPerStep}
   * connections; the line says so when k is above 1.
   *
   * @hidden
   */
  public static List<String> startupLines(
      Collection<? extends SagaRunner<?>> runners, int connectionsPerStep) {
    List<String> lines = new ArrayList<>();
    for (SagaRunner<?> runner : runners) {
      SagaParallelismSnapshot now = runner.parallelismSnapshot();
      List<String> names = now.boundNames();
      StringBuilder bound = new StringBuilder();
      for (String name : names) {
        if (bound.length() > 0) {
          bound.append(", ");
        }
        bound.append('\'').append(LogSanitizer.sanitizeForLog(name)).append('\'');
      }
      int pages = now.concurrentPages();
      lines.add(
          "StreamRune: saga type "
              + runner.sagaType().value()
              + " (subscription "
              + (names.isEmpty() ? "none bound" : bound)
              + "): parallelism "
              + now.value()
              + " ("
              + sourceText(now)
              + ")"
              + (pages > 1 ? " x " + pages + " subscriptions" : "")
              + "; its steps hold up to "
              + (now.value() * pages * connectionsPerStep)
              + " connections at once");
    }
    return lines;
  }

  /**
   * The most steps in flight at once: for each runner its parallelism times its bound subscriptions
   * (at least one), since each subscription delivers pages of its own.
   *
   * @hidden
   */
  public static int stepsInFlightAtMost(Collection<? extends SagaRunner<?>> runners) {
    int sum = 0;
    for (SagaRunner<?> runner : runners) {
      SagaParallelismSnapshot now = runner.parallelismSnapshot();
      sum += now.value() * now.concurrentPages();
    }
    return sum;
  }

  /**
   * The parallelism {@code runner} uses for its next page.
   *
   * @hidden
   */
  public static int effectiveOf(SagaRunner<?> runner) {
    return runner.parallelism();
  }

  /**
   * Where {@code runner}'s parallelism came from.
   *
   * @hidden
   */
  public static SagaParallelismSource sourceOf(SagaRunner<?> runner) {
    return runner.parallelismSource();
  }

  private static String sourceText(SagaParallelismSnapshot now) {
    return switch (now.source()) {
      case SUBSCRIPTION_PROPERTY ->
          "streamrune.saga.subscriptions."
              + LogSanitizer.sanitizeForLog(now.winningSubscription())
              + ".parallelism";
      case BUILDER -> "SagaRunner.Builder.parallelism";
      case GLOBAL_PROPERTY -> "streamrune.saga.parallelism";
      case DEFAULT -> "default";
    };
  }

  private static List<String> sanitized(Set<String> names) {
    return names.stream().map(LogSanitizer::sanitizeForLog).toList();
  }
}
