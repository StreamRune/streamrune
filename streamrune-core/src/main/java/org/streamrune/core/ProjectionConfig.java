package org.streamrune.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorStrategy;

/**
 * Marks a {@link org.streamrune.core.projection.Projection} bean for framework auto-registration.
 *
 * <p>Per-projection properties at {@code streamrune.projections.<name>.*} override annotation
 * values (precedence: properties &gt; annotation) for {@code enabled} and {@code error-strategy};
 * the delivery mode has no property override.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ProjectionConfig {

  /** Projection name. Required. Used as offset-store key + ops handle. */
  String name();

  /** Execution mode. Default {@link Mode#CONTINUOUS}. */
  Mode mode() default Mode.CONTINUOUS;

  /**
   * Cron expression in Spring 6-field format ("sec min hr dom mon dow"). Required iff {@code mode
   * == SCHEDULED}; ignored otherwise.
   */
  String cron() default "";

  /** Error strategy. Default {@link ProjectionErrorStrategy#HALT}. */
  ProjectionErrorStrategy errorStrategy() default ProjectionErrorStrategy.HALT;

  /** Per-projection batch size. {@code -1} = use runner default. */
  int batchSize() default -1;

  /** Per-projection fetch size. {@code -1} = use runner default. */
  int fetchSize() default -1;

  /**
   * The delivery guarantee this projection declares. Required — there is no default: a missing
   * attribute is a compile error. The mode declares what this projection promises; the integration
   * chooses the processor that can honour it, or refuses to start. An INLINE projection declares
   * {@link ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT} — it has no checkpoint to be
   * transactional with; the other two values are refused at discovery.
   *
   * <table>
   *   <caption>Delivery modes</caption>
   *   <tr><th>mode</th><th>framework guarantees</th><th>projection promises</th></tr>
   *   <tr><td>{@code TRANSACTIONAL_LOCAL}</td><td>read-model writes and checkpoint are all-or-nothing;
   *   a redelivered or split-brain batch is rejected before any write</td><td>writes only through the
   *   handed repository during {@code process}</td></tr>
   *   <tr><td>{@code AT_LEAST_ONCE_IDEMPOTENT}</td><td>the checkpoint advances only after {@code
   *   process} returned; a crash or a rejected checkpoint redelivers; the projection is never handed
   *   a transaction-scoped repository</td><td>applying the same event twice yields the same read
   *   model</td></tr>
   *   <tr><td>{@code EXTERNAL_EFFECT}</td><td>as {@code TRANSACTIONAL_LOCAL}, for the outbox row</td>
   *   <td>no external call in {@code process}; the receiver dedups by event id</td></tr>
   * </table>
   *
   * @return the declared delivery mode
   */
  ProjectionDeliveryMode deliveryMode();

  enum Mode {
    /** Continuous polling — runs on {@code MultiProjectionRunner}. */
    CONTINUOUS,
    /** Cron-triggered drain — runs on {@code ScheduledProjectionRunner}. */
    SCHEDULED,
    /**
     * Synchronous after command — runs via {@code InlineProjectionInterceptor}, on the command's
     * own thread, immediately after its events commit.
     *
     * <p><b>Explicit contract:</b> post-commit, same-JVM, and ordered per-aggregate (the command
     * bus holds the aggregate's lock through the projection call, so two commands on the same
     * aggregate apply in commit order — see {@code InlineProjectionInterceptor}'s javadoc). INLINE
     * has <b>no recovery path</b>: a projection failure is logged and swallowed, and the command
     * still reports success. Use CONTINUOUS or SCHEDULED with a transactional repository for any
     * read model that must be authoritative; treat INLINE as a best-effort latency optimization
     * only.
     *
     * <p>An INLINE registration declares {@link ProjectionDeliveryMode#AT_LEAST_ONCE_IDEMPOTENT}:
     * it has no checkpoint to be transactional with, and a failure is logged and the batch is not
     * retried.
     */
    INLINE
  }
}
