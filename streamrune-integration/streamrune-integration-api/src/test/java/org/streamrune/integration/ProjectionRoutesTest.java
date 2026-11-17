package org.streamrune.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.Sort;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.integration.ProjectionStoreRouting.Registration;
import org.streamrune.test.InMemoryProjectionRepository;

class ProjectionRoutesTest {

  private static final CheckpointTarget SUBSCRIPTION =
      new CheckpointTarget(
          "subscription_checkpoint",
          new InMemoryProjectionRepository(),
          AtomicBatchProcessor.nonAtomicAtLeastOnce());

  private static ReadModelStore store(String name) {
    var repository = new InMemoryProjectionRepository();
    return ReadModelStore.of(name, repository, repository, repository);
  }

  private static Projection writingTo(ProjectionRepository target) {
    return new Projection() {
      @Override
      public void process(List<EventEnvelope> batch) {}

      @Override
      public Optional<ProjectionRepository> writeTarget() {
        return Optional.of(target);
      }
    };
  }

  private static ProjectionIndex indexOf(String projection) {
    return ProjectionIndex.forSort(ProjectionName.of(projection), Sort.by("createdAt"));
  }

  @Test
  void routesEveryRegistrationOnce_andKnowsTheStoreBehindEachTarget() {
    var postgres = store("postgres");
    var search = store("search");
    var stores = new ReadModelStores(() -> Optional.of(postgres), () -> List.of(search));

    var routes =
        ProjectionRoutes.route(
            List.of(
                new Registration(
                    "orders", TRANSACTIONAL_LOCAL, "", writingTo(postgres.repository())),
                new Registration("catalog", AT_LEAST_ONCE_IDEMPOTENT, "search", batch -> {}),
                new Registration("audit", AT_LEAST_ONCE_IDEMPOTENT, "", batch -> {})),
            stores,
            () -> SUBSCRIPTION,
            SubscriptionLeadership.NOOP);

    assertThat(routes.target("orders")).isEqualTo(postgres.checkpointTarget());
    assertThat(routes.target("catalog")).isEqualTo(search.checkpointTarget());
    assertThat(routes.target("audit")).isSameAs(SUBSCRIPTION);
    assertThat(routes.store(ProjectionName.of("orders"))).containsSame(postgres);
    assertThat(routes.store(ProjectionName.of("catalog"))).containsSame(search);
    assertThat(routes.store(ProjectionName.of("audit")))
        .as("the subscription checkpoint is no store")
        .isEmpty();
    assertThat(routes.store(ProjectionName.of("unregistered"))).isEmpty();
  }

  @Test
  void anIndexGoesToItsProjectionsRoutedStore_otherwiseToTheFallback() {
    var postgres = store("postgres");
    var search = store("search");
    var stores = new ReadModelStores(() -> Optional.of(postgres), () -> List.of(search));
    var routes =
        ProjectionRoutes.route(
            List.of(
                new Registration("catalog", AT_LEAST_ONCE_IDEMPOTENT, "search", batch -> {}),
                new Registration("audit", AT_LEAST_ONCE_IDEMPOTENT, "", batch -> {})),
            stores,
            () -> SUBSCRIPTION,
            SubscriptionLeadership.NOOP);
    var fallbackCalls = new AtomicInteger();
    ProjectionRepository fallback = new InMemoryProjectionRepository();

    assertThat(
            routes.indexRepository(
                indexOf("catalog"),
                () -> {
                  fallbackCalls.incrementAndGet();
                  return fallback;
                }))
        .isSameAs(search.repository());
    assertThat(fallbackCalls).as("a routed index never asks for the fallback").hasValue(0);
    assertThat(routes.indexRepository(indexOf("audit"), () -> fallback)).isSameAs(fallback);
    assertThat(routes.indexRepository(indexOf("unregistered"), () -> fallback)).isSameAs(fallback);
  }

  @Test
  void noRegistrations_touchNeitherTheStoresNorTheSubscriptionCheckpoint() {
    var selections = new AtomicInteger();
    var stores =
        new ReadModelStores(
            () -> {
              selections.incrementAndGet();
              return Optional.empty();
            },
            List::of);

    var routes =
        ProjectionRoutes.route(
            List.of(),
            stores,
            () -> {
              throw new AssertionError("no subscription checkpoint needed");
            },
            SubscriptionLeadership.NOOP);

    assertThat(selections).hasValue(0);
    var fallback = new InMemoryProjectionRepository();
    assertThat(routes.indexRepository(indexOf("orders"), () -> fallback)).isSameAs(fallback);
    assertThat(ProjectionRoutes.none().store(ProjectionName.of("orders"))).isEmpty();
  }

  @Test
  void aRoutingFailure_isThrown() {
    var stores = new ReadModelStores(Optional::empty, () -> List.of(store("search")));

    assertThatThrownBy(
            () ->
                ProjectionRoutes.route(
                    List.of(new Registration("orders", AT_LEAST_ONCE_IDEMPOTENT, "nope", b -> {})),
                    stores,
                    () -> SUBSCRIPTION,
                    SubscriptionLeadership.NOOP))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("declares readModelStore = \"nope\"");
  }

  @Test
  void anUnroutedProjection_hasNoTarget() {
    assertThatThrownBy(() -> ProjectionRoutes.none().target("orders"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("projection 'orders' was not routed");
  }

  @Test
  void refusesNulls() {
    var stores = new ReadModelStores(Optional::empty, List::of);
    assertThatThrownBy(
            () ->
                ProjectionRoutes.route(
                    null, stores, () -> SUBSCRIPTION, SubscriptionLeadership.NOOP))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("registrations is required");
    assertThatThrownBy(
            () ->
                ProjectionRoutes.route(
                    List.of(), null, () -> SUBSCRIPTION, SubscriptionLeadership.NOOP))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("stores is required");
    assertThatThrownBy(
            () -> ProjectionRoutes.route(List.of(), stores, null, SubscriptionLeadership.NOOP))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("subscriptionCheckpoint is required");
    assertThatThrownBy(() -> ProjectionRoutes.route(List.of(), stores, () -> SUBSCRIPTION, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("leadership is required");
    assertThatThrownBy(() -> ProjectionRoutes.none().indexRepository(null, () -> null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("index is required");
    assertThatThrownBy(() -> ProjectionRoutes.none().indexRepository(indexOf("a"), null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("fallback is required");
  }
}
