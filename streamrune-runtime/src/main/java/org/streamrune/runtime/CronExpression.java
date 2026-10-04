package org.streamrune.runtime;

import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

/**
 * Wrapper around cron-utils for Spring 6-field cron expressions ("sec min hr dom mon dow"). Parses
 * eagerly; computes next fire instant relative to a given moment.
 */
public final class CronExpression {

  private static final CronParser PARSER =
      new CronParser(CronDefinitionBuilder.instanceDefinitionFor(CronType.SPRING));

  private final ExecutionTime executionTime;
  private final ZoneId zoneId;
  private final String original;

  private CronExpression(ExecutionTime executionTime, ZoneId zoneId, String original) {
    this.executionTime = executionTime;
    this.zoneId = zoneId;
    this.original = original;
  }

  /**
   * Parses the given Spring 6-field cron expression in the given zone.
   *
   * @throws IllegalArgumentException if {@code expression} is null/blank or invalid
   */
  public static CronExpression parse(String expression, ZoneId zoneId) {
    if (expression == null) {
      throw new IllegalArgumentException("cron expression is null");
    }
    if (expression.isBlank()) {
      throw new IllegalArgumentException("cron expression is blank");
    }
    if (zoneId == null) {
      throw new IllegalArgumentException("zoneId is required");
    }
    try {
      var cron = PARSER.parse(expression);
      return new CronExpression(ExecutionTime.forCron(cron), zoneId, expression);
    } catch (IllegalArgumentException | IllegalStateException e) {
      // cron-utils throws CronParseException (extends IllegalArgumentException) for invalid
      // syntax and IllegalStateException for some internal validation failures.
      throw new IllegalArgumentException("invalid cron expression: " + expression, e);
    }
  }

  /**
   * Returns the next fire instant strictly after {@code from}, or {@link Instant#MAX} if no future
   * fire exists (e.g. impossible date like {@code "0 0 0 30 2 *"}).
   *
   * <p><b>Caution:</b> {@code Instant.MAX} is a sentinel — callers MUST check for it before doing
   * arithmetic. {@code nextFire.plus(...)} will throw {@code DateTimeException}; {@code
   * Duration.between(now, Instant.MAX)} returns a near-{@code Long.MAX_VALUE} value.
   */
  public Instant nextFire(Instant from) {
    ZonedDateTime fromZdt = from.atZone(zoneId);
    Optional<ZonedDateTime> next = executionTime.nextExecution(fromZdt);
    return next.map(ZonedDateTime::toInstant).orElse(Instant.MAX);
  }

  @Override
  public String toString() {
    return original;
  }
}
