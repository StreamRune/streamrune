package org.streamrune.integration;

import org.streamrune.core.types.LogSanitizer;

/**
 * The exception messages the three framework integrations raise when projection discovery rejects
 * an application's configuration, in one place so the wording and the sanitizing cannot drift
 * between Spring, Quarkus and Micronaut.
 *
 * <p>Every value interpolated here — a projection name from {@code @ProjectionConfig.name}, an
 * {@code error-strategy} string from the application's configuration — is application-supplied
 * text, and a startup failure message is rendered by every log appender and orchestrator event that
 * quotes it. Names therefore pass through {@link LogSanitizer#sanitizeForLog(String)} and free
 * configuration text through {@link LogSanitizer#sanitizeFreeText(String)}; the messages stay
 * readable for any ordinary name.
 */
public final class ProjectionDiscoveryMessages {

  private ProjectionDiscoveryMessages() {}

  /**
   * Message for two projections declaring the same {@code @ProjectionConfig.name}.
   *
   * @param name the duplicated name
   * @return the message
   */
  public static String duplicateName(String name) {
    return "Duplicate @ProjectionConfig.name: '" + LogSanitizer.sanitizeForLog(name) + "'";
  }

  /**
   * Message for a SCHEDULED projection that declares no cron expression.
   *
   * @param name the projection's name
   * @return the message
   */
  public static String cronRequired(String name) {
    return "@ProjectionConfig.cron is required for SCHEDULED mode (projection '"
        + LogSanitizer.sanitizeForLog(name)
        + "')";
  }

  /**
   * Message for an {@code error-strategy} property that is not a known strategy.
   *
   * @param value the configured text
   * @param name the projection's name
   * @return the message
   */
  public static String invalidErrorStrategy(String value, String name) {
    return "Invalid error-strategy '"
        + LogSanitizer.sanitizeFreeText(value)
        + "' for projection '"
        + LogSanitizer.sanitizeForLog(name)
        + "'";
  }
}
