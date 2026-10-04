package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import io.opentelemetry.api.OpenTelemetry;
import jakarta.validation.Validator;
import java.lang.reflect.Proxy;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.runtime.ProjectionDeliveryPolicy.Verdict;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * One method per row of the delivery-mode decision matrix. The runners' own tests pin that each of
 * them calls this.
 */
class ProjectionDeliveryPolicyTest {

  private static final AtomicBatchProcessor NON_ATOMIC =
      AtomicBatchProcessor.nonAtomicAtLeastOnce();

  /** A plain projection: lambda, inherits the two-arg default, exposes no write target. */
  private static final Projection PLAIN = batch -> {};

  /** Writes through the handed repository but exposes no write target (a custom projection). */
  private static final class WriteThroughNoTarget implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}

    @Override
    public void process(List<EventEnvelope> batch, ProjectionRepository repository) {
      repository.save(ProjectionName.of("x"), "id", "row");
    }
  }

  private static BaseProjection over(ProjectionRepository repo) {
    return new BaseProjection(repo, "orders") {
      @Override
      public void process(List<EventEnvelope> batch) {}
    };
  }

  private static Verdict require(
      Projection p, ProjectionDeliveryMode m, AtomicBatchProcessor proc) {
    return ProjectionDeliveryPolicy.require(p, m, proc, "TestRunner", "orders");
  }

  @ParameterizedTest
  @EnumSource(names = {"TRANSACTIONAL_LOCAL", "EXTERNAL_EFFECT"})
  void transactionalMode_underANonFencingProcessor_isRefused(ProjectionDeliveryMode mode) {
    var repo = new InMemoryProjectionRepository();
    assertThatThrownBy(() -> require(over(repo), mode, NON_ATOMIC))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("TestRunner")
        .hasMessageContaining("'orders'")
        .hasMessageContaining(mode.name())
        .hasMessageContaining("needs a transactional processor")
        .hasMessageContaining("nonAtomicAtLeastOnce")
        .hasMessageContaining("AT_LEAST_ONCE_IDEMPOTENT");
  }

  @ParameterizedTest
  @EnumSource(names = {"TRANSACTIONAL_LOCAL", "EXTERNAL_EFFECT"})
  void transactionalMode_withANonWriteThroughProjection_isRefused(ProjectionDeliveryMode mode) {
    var processor = new InMemoryProjectionRepository();
    assertThatThrownBy(() -> require(PLAIN, mode, processor))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("does not write through the handed repository")
        .hasMessageContaining("INHERITED DEFAULT")
        .hasMessageContaining("declare AT_LEAST_ONCE_IDEMPOTENT");
  }

  @ParameterizedTest
  @EnumSource(names = {"TRANSACTIONAL_LOCAL", "EXTERNAL_EFFECT"})
  void transactionalMode_writeTargetIsAnotherStore_isRefused_namingBoth(
      ProjectionDeliveryMode mode) {
    var processor = new InMemoryProjectionRepository();
    var other = new InMemoryProjectionRepository();
    assertThatThrownBy(() -> require(over(other), mode, processor))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declares " + mode.name() + " and writes to")
        .hasMessageContaining(InMemoryProjectionRepository.class.getName())
        .hasMessageContaining(
            "its writes inside process() would go into the processor's transaction")
        .hasMessageContaining("Pass the repository it writes to as the processor")
        .hasMessageContaining("or declare AT_LEAST_ONCE_IDEMPOTENT")
        .hasMessageContaining("same DataSource, different ObjectMapper");
  }

  @ParameterizedTest
  @EnumSource(names = {"TRANSACTIONAL_LOCAL", "EXTERNAL_EFFECT"})
  void transactionalMode_writeTargetIsTheProcessor_isVerified(ProjectionDeliveryMode mode) {
    var processor = new InMemoryProjectionRepository();
    assertThat(require(over(processor), mode, processor)).isEqualTo(Verdict.VERIFIED);
  }

  @ParameterizedTest
  @EnumSource(names = {"TRANSACTIONAL_LOCAL", "EXTERNAL_EFFECT"})
  void transactionalMode_writeThroughWithoutATarget_isDeclared(ProjectionDeliveryMode mode) {
    assertThat(require(new WriteThroughNoTarget(), mode, new InMemoryProjectionRepository()))
        .isEqualTo(Verdict.DECLARED);
  }

  @Test
  void atLeastOnce_isAcceptedUnderEveryPairing() {
    var processor = new InMemoryProjectionRepository();
    var other = new InMemoryProjectionRepository();
    assertThat(require(PLAIN, AT_LEAST_ONCE_IDEMPOTENT, NON_ATOMIC))
        .isEqualTo(Verdict.AT_LEAST_ONCE);
    assertThat(require(PLAIN, AT_LEAST_ONCE_IDEMPOTENT, processor))
        .isEqualTo(Verdict.AT_LEAST_ONCE);
    assertThat(require(over(other), AT_LEAST_ONCE_IDEMPOTENT, processor))
        .as(
            "BaseProjection over ANOTHER store: accepted, no identity check (the runner hands null)")
        .isEqualTo(Verdict.AT_LEAST_ONCE);
    assertThat(require(over(other), AT_LEAST_ONCE_IDEMPOTENT, NON_ATOMIC))
        .isEqualTo(Verdict.AT_LEAST_ONCE);
    assertThat(require(over(processor), AT_LEAST_ONCE_IDEMPOTENT, processor))
        .as("the understatement is accepted and only named")
        .isEqualTo(Verdict.AT_LEAST_ONCE_TRANSACTIONAL_AVAILABLE);
  }

  @Test
  void decoratedProjection_forwardsWriteTarget_soTheIdentityCheckStillRuns() {
    var processor = new InMemoryProjectionRepository();
    var other = new InMemoryProjectionRepository();
    Projection decorated =
        new TracingProjectionDecorator(
            ProjectionName.of("orders"), over(other), OpenTelemetry.noop());
    assertThatThrownBy(() -> require(decorated, TRANSACTIONAL_LOCAL, processor))
        .hasMessageContaining("writes to");
    Projection decoratedOk =
        new ValidatingProjectionDecorator(over(processor), mock(Validator.class));
    assertThat(require(decoratedOk, TRANSACTIONAL_LOCAL, processor)).isEqualTo(Verdict.VERIFIED);
  }

  @Test
  void proxiedProcessorAndProxiedRepository_verify() {
    var real = new InMemoryProjectionRepository();
    Object proxy =
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {ProjectionRepository.class, AtomicBatchProcessor.class},
            (p, m, a) -> m.invoke(real, a));
    var projection = over((ProjectionRepository) proxy);
    assertThat(require(projection, TRANSACTIONAL_LOCAL, (AtomicBatchProcessor) proxy))
        .isEqualTo(Verdict.VERIFIED);
    assertThat(require(projection, TRANSACTIONAL_LOCAL, real)).isEqualTo(Verdict.VERIFIED);
  }

  @Test
  void nullProcessorAndNullModeAreRefused_withTheTwoWaysOut() {
    assertThatThrownBy(() -> require(PLAIN, AT_LEAST_ONCE_IDEMPOTENT, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("atomicProcessor is required")
        .hasMessageContaining("JdbcProjectionRepository")
        .hasMessageContaining("AtomicBatchProcessor.nonAtomicAtLeastOnce()");
    assertThatThrownBy(() -> require(PLAIN, null, NON_ATOMIC))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("deliveryMode is required");
    assertThatThrownBy(() -> require(null, AT_LEAST_ONCE_IDEMPOTENT, NON_ATOMIC))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("'orders'")
        .hasMessageContaining("projection is required");
  }

  /**
   * A fencing processor that is not itself a repository (a test double, or a wrapper) has only its
   * own identity: a BaseProjection over any store is refused, and the message prints the processor.
   */
  @Test
  void aFencingProcessorThatIsNoRepository_refusesEveryExposedWriteTarget_namingItself() {
    AtomicBatchProcessor fencingOnly = TestFencingProcessor.fencingClaimOnly();
    var other = new InMemoryProjectionRepository();
    assertThatThrownBy(() -> require(over(other), TRANSACTIONAL_LOCAL, fencingOnly))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("declares TRANSACTIONAL_LOCAL and writes to")
        .hasMessageContaining(fencingOnly.getClass().getName())
        .hasMessageContaining("(identity " + fencingOnly + ")");
    assertThat(require(new WriteThroughNoTarget(), TRANSACTIONAL_LOCAL, fencingOnly))
        .isEqualTo(Verdict.DECLARED);
  }

  @Test
  void verdictsDescribeThemselves_andTheProcessorLabelNamesTheNonatomicOne() {
    assertThat(Verdict.VERIFIED.describe()).isEqualTo("transactional, write target verified");
    assertThat(Verdict.DECLARED.describe())
        .isEqualTo("transactional, write-through declared (write target not exposed)");
    assertThat(Verdict.AT_LEAST_ONCE.describe())
        .isEqualTo(
            "at-least-once: no transaction-scoped repository is handed; read-model writes are"
                + " outside the checkpoint transaction");
    assertThat(Verdict.AT_LEAST_ONCE_TRANSACTIONAL_AVAILABLE.describe())
        .isEqualTo(
            "at-least-once: this projection writes to the processor's own store, so"
                + " TRANSACTIONAL_LOCAL is available at no cost");
    assertThat(ProjectionDeliveryPolicy.processorLabel(NON_ATOMIC))
        .isEqualTo("nonAtomicAtLeastOnce");
    assertThat(ProjectionDeliveryPolicy.processorLabel(new InMemoryProjectionRepository()))
        .isEqualTo(InMemoryProjectionRepository.class.getName());
  }

  @Test
  void projectionNameIsSanitizedInEveryMessage() {
    assertThatThrownBy(
            () ->
                ProjectionDeliveryPolicy.require(
                    PLAIN, TRANSACTIONAL_LOCAL, NON_ATOMIC, "TestRunner", "bad\nname"))
        .satisfies(ex -> assertThat(ex.getMessage()).doesNotContain("\n"));
  }
}
