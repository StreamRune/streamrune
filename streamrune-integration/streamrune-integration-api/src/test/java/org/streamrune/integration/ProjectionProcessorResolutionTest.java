package org.streamrune.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.EXTERNAL_EFFECT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.integration.ProjectionProcessorResolution.Registration;

class ProjectionProcessorResolutionTest {

  private static final AtomicBatchProcessor BEAN = (n, b, o, e, u, s) -> {};
  private static final AtomicBatchProcessor OTHER_BEAN = (n, b, o, e, u, s) -> {};

  /**
   * Anything but NOOP. A stub, not a mock: streamrune-integration-api has no Mockito on its test
   * classpath (its build declares only the root's junit + archunit and a Prometheus registry).
   */
  private static final class RealLeadership implements SubscriptionLeadership {
    @Override
    public java.util.Optional<Lease> tryAcquire(String consumerName) {
      return java.util.Optional.of(new Lease(1L));
    }

    @Override
    public java.util.Optional<Lease> current(String consumerName) {
      return java.util.Optional.of(new Lease(1L));
    }

    @Override
    public void resign(String consumerName) {}

    @Override
    public void close() {}
  }

  private static final SubscriptionLeadership REAL = new RealLeadership();

  @Test
  void allAtLeastOnce_noLeadership_selectsTheNonatomicProcessor_evenWhenABeanExists() {
    var regs =
        List.of(
            new Registration("a", AT_LEAST_ONCE_IDEMPOTENT),
            new Registration("b", AT_LEAST_ONCE_IDEMPOTENT));
    assertThat(ProjectionProcessorResolution.select(regs, null, List.of(BEAN), "runner"))
        .isSameAs(AtomicBatchProcessor.nonAtomicAtLeastOnce());
    assertThat(
            ProjectionProcessorResolution.select(
                regs, SubscriptionLeadership.NOOP, List.of(), "runner"))
        .isSameAs(AtomicBatchProcessor.nonAtomicAtLeastOnce());
  }

  @Test
  void aTransactionalRegistration_selectsTheSingleBean() {
    var regs =
        List.of(
            new Registration("orders", TRANSACTIONAL_LOCAL),
            new Registration("audit", AT_LEAST_ONCE_IDEMPOTENT));
    assertThat(ProjectionProcessorResolution.select(regs, null, List.of(BEAN), "runner"))
        .isSameAs(BEAN);
    assertThat(
            ProjectionProcessorResolution.select(
                List.of(new Registration("fx", EXTERNAL_EFFECT)), null, List.of(BEAN), "runner"))
        .isSameAs(BEAN);
  }

  @Test
  void realLeadership_selectsTheSingleBean_forFencing_evenWhenEveryRegistrationIsAtLeastOnce() {
    var regs = List.of(new Registration("audit", AT_LEAST_ONCE_IDEMPOTENT));
    assertThat(ProjectionProcessorResolution.select(regs, REAL, List.of(BEAN), "runner"))
        .isSameAs(BEAN);
  }

  @Test
  void transactionalRegistration_withoutABean_failsNamingTheRegistrationsAndTheFix() {
    var regs =
        List.of(
            new Registration("orders", TRANSACTIONAL_LOCAL),
            new Registration("audit", AT_LEAST_ONCE_IDEMPOTENT),
            new Registration("fx", EXTERNAL_EFFECT));
    assertThatThrownBy(
            () ->
                ProjectionProcessorResolution.select(
                    regs, null, List.of(), "streamRuneMultiProjectionRunner"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "streamRuneMultiProjectionRunner: projection 'orders' declares TRANSACTIONAL_LOCAL and"
                + " projection 'fx' declares EXTERNAL_EFFECT, but no AtomicBatchProcessor bean is"
                + " available. Define one — org.streamrune.postgres.JdbcProjectionRepository over"
                + " the DataSource whose projection_offset table the runner's OffsetStore reads,"
                + " declared with its CONCRETE type — or declare these projections"
                + " AT_LEAST_ONCE_IDEMPOTENT.");
  }

  @Test
  void singleTransactionalRegistration_withoutABean_failsWithTheSingularFix() {
    var regs = List.of(new Registration("orders", TRANSACTIONAL_LOCAL));
    assertThatThrownBy(() -> ProjectionProcessorResolution.select(regs, null, List.of(), "runner"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "runner: projection 'orders' declares TRANSACTIONAL_LOCAL, but no AtomicBatchProcessor"
                + " bean is available. Define one — org.streamrune.postgres.JdbcProjectionRepository"
                + " over the DataSource whose projection_offset table the runner's OffsetStore"
                + " reads, declared with its CONCRETE type — or declare the projection"
                + " AT_LEAST_ONCE_IDEMPOTENT.");
  }

  @Test
  void realLeadership_withoutABean_failsNamingTheLeadership() {
    var regs = List.of(new Registration("audit", AT_LEAST_ONCE_IDEMPOTENT));
    assertThatThrownBy(() -> ProjectionProcessorResolution.select(regs, REAL, List.of(), "runner"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "runner: single-active-consumer leadership ("
                + RealLeadership.class.getName()
                + ") is on, but no AtomicBatchProcessor bean is available to honour its epoch."
                + " Define one — org.streamrune.postgres.JdbcProjectionRepository over the"
                + " DataSource whose projection_offset table the runner's OffsetStore reads,"
                + " declared with its CONCRETE type — or disable leadership for a single instance"
                + " (streamrune.subscription.single-active-consumer.enabled=false).");
  }

  /**
   * Both needs at once: the message names the transactional registration and the leadership, and
   * offers both fixes — a bean satisfies the two, while declaring the projection at-least-once
   * alone would still leave the epoch unfenced.
   */
  @Test
  void transactionalRegistration_andRealLeadership_withoutABean_failsNamingBothAndBothFixes() {
    var regs = List.of(new Registration("orders", TRANSACTIONAL_LOCAL));
    assertThatThrownBy(() -> ProjectionProcessorResolution.select(regs, REAL, List.of(), "runner"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "runner: projection 'orders' declares TRANSACTIONAL_LOCAL and single-active-consumer"
                + " leadership ("
                + RealLeadership.class.getName()
                + ") is on, but no AtomicBatchProcessor bean is available to honour its epoch.")
        .hasMessageContaining(
            "or declare the projection AT_LEAST_ONCE_IDEMPOTENT, and disable leadership for a single"
                + " instance (streamrune.subscription.single-active-consumer.enabled=false)");
  }

  @Test
  void ambiguousBeans_failNamingThem() {
    var regs = List.of(new Registration("orders", TRANSACTIONAL_LOCAL));
    assertThatThrownBy(
            () ->
                ProjectionProcessorResolution.select(
                    regs, null, List.of(BEAN, OTHER_BEAN), "runner"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("2 AtomicBatchProcessor beans")
        .hasMessageContaining(BEAN.getClass().getName())
        .hasMessageContaining(OTHER_BEAN.getClass().getName())
        .hasMessageContaining("a runner of their own");
  }

  @Test
  void inlineRequiresAtLeastOnce() {
    ProjectionProcessorResolution.requireInlineMode("x", AT_LEAST_ONCE_IDEMPOTENT);
    for (var mode : List.of(TRANSACTIONAL_LOCAL, EXTERNAL_EFFECT)) {
      assertThatThrownBy(() -> ProjectionProcessorResolution.requireInlineMode("x", mode))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(
              "projection 'x' is INLINE and declares "
                  + mode
                  + ": INLINE runs post-commit on the command"
                  + " thread with no checkpoint and no transaction of its own. Declare"
                  + " AT_LEAST_ONCE_IDEMPOTENT, or run it CONTINUOUS or SCHEDULED.");
    }
  }

  @Test
  void projectionNamesAreSanitized() {
    assertThatThrownBy(
            () -> ProjectionProcessorResolution.requireInlineMode("bad\nname", TRANSACTIONAL_LOCAL))
        .satisfies(ex -> assertThat(ex.getMessage()).doesNotContain("\n"));
  }
}
