package org.streamrune.integration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import org.streamrune.core.Internal;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.integration.ProjectionStoreRouting.Registration;

/**
 * Where an application's runner registrations are routed, computed once: each framework integration
 * routes every {@code CONTINUOUS} and {@code SCHEDULED} registration it discovers with {@link
 * ProjectionStoreRouting#route} into one bean of this type, and both of its readers use that one
 * result — the projection runners (each registration's {@link #target target}) and the index
 * declaration (each {@link ProjectionIndex} goes to the store its projection is routed to, {@link
 * #indexRepository}).
 *
 * @hidden
 */
@Internal
public final class ProjectionRoutes {

  private static final ProjectionRoutes NONE = new ProjectionRoutes(Map.of(), Map.of());

  private final Map<String, CheckpointTarget> targets;
  private final Map<String, ReadModelStore> stores;

  private ProjectionRoutes(
      Map<String, CheckpointTarget> targets, Map<String, ReadModelStore> stores) {
    this.targets = Map.copyOf(targets);
    this.stores = Map.copyOf(stores);
  }

  /**
   * No registration routed: projection auto-discovery is off, or no runner registration exists.
   *
   * @return the empty routes
   */
  public static ProjectionRoutes none() {
    return NONE;
  }

  /**
   * Routes every registration once. Without registrations nothing is touched: neither the stores
   * (no store is created) nor the subscription checkpoint.
   *
   * @param registrations the runners' registrations, each projection name once
   * @param stores the application's read-model stores
   * @param subscriptionCheckpoint supplies the event store's subscription checkpoint, rule 4's
   *     target; asked for once, and only when there are registrations
   * @param leadership the configured leadership, or {@link SubscriptionLeadership#NOOP}
   * @return the routes
   * @throws NullPointerException if an argument is {@code null}
   * @throws IllegalStateException if the store set is invalid or a registration cannot be routed
   *     ({@link ProjectionStoreRouting#route})
   */
  public static ProjectionRoutes route(
      List<Registration> registrations,
      ReadModelStores stores,
      Supplier<CheckpointTarget> subscriptionCheckpoint,
      SubscriptionLeadership leadership) {
    Objects.requireNonNull(registrations, "registrations is required");
    Objects.requireNonNull(stores, "stores is required");
    Objects.requireNonNull(subscriptionCheckpoint, "subscriptionCheckpoint is required");
    Objects.requireNonNull(leadership, "leadership is required");
    if (registrations.isEmpty()) {
      return NONE;
    }
    List<ReadModelStore> candidates = stores.stores();
    CheckpointTarget subscription = subscriptionCheckpoint.get();
    var targets = new LinkedHashMap<String, CheckpointTarget>();
    var routedStores = new LinkedHashMap<String, ReadModelStore>();
    for (Registration registration : registrations) {
      var routed =
          ProjectionStoreRouting.resolve(registration, candidates, subscription, leadership);
      targets.put(registration.projectionName(), routed.target());
      if (routed.store() != null) {
        routedStores.put(registration.projectionName(), routed.store());
      }
    }
    return new ProjectionRoutes(targets, routedStores);
  }

  /**
   * The target a registration was routed to.
   *
   * @param projectionName the registration's projection name
   * @return the pair its runner reads and commits through
   * @throws IllegalStateException if no registration of that name was routed
   */
  public CheckpointTarget target(String projectionName) {
    CheckpointTarget target = targets.get(projectionName);
    if (target == null) {
      throw new IllegalStateException(
          "projection '"
              + LogSanitizer.sanitizeForLog(projectionName)
              + "' was not routed: it is not a registration of the framework's projection"
              + " runners");
    }
    return target;
  }

  /**
   * The read-model store a projection's registration was routed to.
   *
   * @param projection the projection
   * @return its store; empty when it was routed to the event store's subscription checkpoint, or
   *     has no runner registration
   */
  public Optional<ReadModelStore> store(ProjectionName projection) {
    return Optional.ofNullable(stores.get(projection.value()));
  }

  /**
   * The repository an index is declared on: the repository of the store its projection is routed
   * to; for a projection routed to no store — the subscription checkpoint, an INLINE projection, or
   * no registration at all — {@code fallback}, the integration's injectable {@code
   * ProjectionRepository} bean (the {@value ProjectionStoreRouting#DEFAULT_STORE} store's
   * repository unless the application defines its own).
   *
   * @param index the index to declare
   * @param fallback the repository for an index routed to no store; asked for only then
   * @return the repository to declare {@code index} on
   * @throws NullPointerException if an argument is {@code null}
   */
  public ProjectionRepository indexRepository(
      ProjectionIndex index, Supplier<ProjectionRepository> fallback) {
    Objects.requireNonNull(index, "index is required");
    Objects.requireNonNull(fallback, "fallback is required");
    return store(index.projection()).map(ReadModelStore::repository).orElseGet(fallback);
  }
}
