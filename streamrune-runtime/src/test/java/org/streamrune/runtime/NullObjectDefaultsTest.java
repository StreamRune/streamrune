package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.streamrune.core.QueryBus;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLifecycle;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.runtime.gdpr.ForgetSubjectService;

/**
 * The optional collaborators of the runtime's builders and constructors (metrics, leadership,
 * health contributor, tracing, upcasters) take no {@code null}: each defaults to its empty value,
 * and a {@code null} argument fails at once with a message that names it.
 */
class NullObjectDefaultsTest {

  private static final SubscriptionName NAME = SubscriptionName.of("orders");

  @Test
  void theNoopHealthContributor_registersNothing_recordsNothing_andReportsNoSubscription() {
    SubscriptionLifecycle lifecycle = mock(SubscriptionLifecycle.class);
    SubscriptionHealthContributor noop = SubscriptionHealthContributor.NOOP;

    noop.register(NAME, lifecycle);
    noop.replaceLifecycle(NAME, lifecycle);
    noop.recordError(NAME);
    noop.recordSuccess(NAME);
    noop.markTerminalError(NAME);

    assertThat(noop.health()).isEmpty();
    assertThat(noop.overallStatus()).isEqualTo(SubscriptionHealth.Status.UP);
    assertThat(noop.currentLag(NAME)).isZero();
    assertThat(noop.lagThreshold()).isEqualTo(SubscriptionHealthContributor.DEFAULT_LAG_THRESHOLD);
    noop.unregister(NAME);
    assertThat(noop.health()).isEmpty();
    verifyNoInteractions(lifecycle);
  }

  @Test
  void healthTrackingUnderTheNoopContributor_leavesTheListenerUnwrapped() {
    EventListener listener = events -> {};

    assertThat(
            HealthTrackingEventListener.tracking(
                listener, SubscriptionHealthContributor.NOOP, NAME))
        .isSameAs(listener);
  }

  @Test
  void healthTrackingUnderARealContributor_wrapsTheListenerOnce() {
    EventListener listener = events -> {};
    var contributor = mock(SubscriptionHealthContributor.class);

    EventListener tracked = HealthTrackingEventListener.tracking(listener, contributor, NAME);

    assertThat(tracked).isInstanceOf(HealthTrackingEventListener.class);
    assertThat(HealthTrackingEventListener.tracking(tracked, contributor, NAME)).isSameAs(tracked);
  }

  static Stream<Arguments> nullCollaborators() {
    return Stream.of(
        refused(
            "healthContributor",
            "HealthTrackingEventListener.tracking",
            () -> HealthTrackingEventListener.tracking(events -> {}, null, NAME)),
        refused(
            "healthContributor",
            "HealthTrackingEventListener",
            () -> new HealthTrackingEventListener(events -> {}, null, NAME)),
        refused(
            "healthContributor",
            "PollingEventSubscription.Builder",
            () -> PollingEventSubscription.builder().healthContributor(null)),
        refused(
            "healthContributor",
            "ContinuousProjectionRunner.Builder",
            () -> ContinuousProjectionRunner.builder().healthContributor(null)),
        refused(
            "healthContributor",
            "MultiProjectionRunner.Builder",
            () -> MultiProjectionRunner.builder().healthContributor(null)),
        refused(
            "healthContributor",
            "ScheduledProjectionRunner.Builder",
            () -> ScheduledProjectionRunner.builder().healthContributor(null)),
        refused(
            "metrics",
            "PollingEventSubscription.Builder",
            () -> PollingEventSubscription.builder().metrics(null)),
        refused(
            "metrics",
            "StreamEventSubscription.Builder",
            () -> StreamEventSubscription.builder().metrics(null)),
        refused(
            "metrics",
            "ContinuousProjectionRunner.Builder",
            () -> ContinuousProjectionRunner.builder().metrics(null)),
        refused(
            "metrics",
            "MultiProjectionRunner.Builder",
            () -> MultiProjectionRunner.builder().metrics(null)),
        refused(
            "metrics",
            "ScheduledProjectionRunner.Builder",
            () -> ScheduledProjectionRunner.builder().metrics(null)),
        refused(
            "metrics",
            "VirtualThreadCommandBus.Builder",
            () -> VirtualThreadCommandBus.builder().metrics(null)),
        refused("metrics", "StreamRune.Builder", () -> StreamRune.builder().metrics(null)),
        refused("metrics", "OutboxPoller.Builder", () -> OutboxPoller.builder().metrics(null)),
        refused(
            "metrics",
            "DeadLetterRetryRunner.Builder",
            () -> DeadLetterRetryRunner.builder().metrics(null)),
        refused(
            "metrics", "SagaRunner.Builder", () -> SagaRunner.<SagaState>builder().metrics(null)),
        refused(
            "metrics",
            "SagaTimeoutRunner.Builder",
            () -> SagaTimeoutRunner.<SagaState>builder().metrics(null)),
        refused(
            "metrics",
            "SagaCompensationRetrySweeper.Builder",
            () -> SagaCompensationRetrySweeper.<SagaState>builder().metrics(null)),
        refused(
            "metrics",
            "SagaDeadLetterReplayer.Builder",
            () -> SagaDeadLetterReplayer.builder().metrics(null)),
        refused(
            "metrics",
            "ForgetSubjectService.Builder",
            () -> ForgetSubjectService.builder().metrics(null)),
        refused("metrics", "CachingQueryBusBuilder", () -> CachingQueryBus.builder().metrics(null)),
        refused("metrics", "SimpleQueryBus", () -> new SimpleQueryBus(null)),
        refused(
            "metrics",
            "AuditingQueryBus",
            () -> new AuditingQueryBus(mock(QueryBus.class), mock(AuditStore.class), null)),
        refused(
            "metrics",
            "SubscriptionHealthContributor",
            () -> new SubscriptionHealthContributor(null, null, null)),
        refused(
            "metrics",
            "DeadLetterRetryRunner.createObjectMapper",
            () -> DeadLetterRetryRunner.createObjectMapper(null, null)),
        refused(
            "leadership",
            "PollingEventSubscription.Builder",
            () -> PollingEventSubscription.builder().leadership(null)),
        refused(
            "leadership",
            "ContinuousProjectionRunner.Builder",
            () -> ContinuousProjectionRunner.builder().leadership(null)),
        refused(
            "leadership",
            "MultiProjectionRunner.Builder",
            () -> MultiProjectionRunner.builder().leadership(null)),
        refused(
            "leadership",
            "ScheduledProjectionRunner.Builder",
            () -> ScheduledProjectionRunner.builder().leadership(null)),
        refused(
            "leadership",
            "DeadLetterRetryRunner.Builder",
            () -> DeadLetterRetryRunner.builder().leadership(null)),
        refused(
            "leadership",
            "SagaRunner.Builder",
            () -> SagaRunner.<SagaState>builder().leadership(null)),
        refused(
            "leadership",
            "SagaCompensationRetrySweeper.Builder",
            () -> SagaCompensationRetrySweeper.<SagaState>builder().leadership(null)),
        refused(
            "leadership",
            "SagaLeaseWiring.warnUngatedUnderLeadership",
            () -> SagaLeaseWiring.warnUngatedUnderLeadership(List.of(), null)),
        refused(
            "tracing",
            "TracingEventListener.traced",
            () -> TracingEventListener.traced(events -> {}, null, NAME)),
        refused(
            "tracing",
            "TracingProjectionDecorator.traced",
            () -> TracingProjectionDecorator.traced(ProjectionName.of("p"), batch -> {}, null)),
        refused("upcasters", "StreamRune.Builder", () -> StreamRune.builder().upcasters(null)));
  }

  @ParameterizedTest(name = "{1} refuses a null {0}")
  @MethodSource("nullCollaborators")
  void aNullCollaboratorIsRefused_withItsName(
      String collaborator, String site, ThrowingCallable call) {
    assertThatThrownBy(call)
        .isInstanceOf(NullPointerException.class)
        .hasMessage(collaborator + " is required");
  }

  private static Arguments refused(String collaborator, String site, ThrowingCallable call) {
    return Arguments.of(collaborator, site, call);
  }
}
