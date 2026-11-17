package org.streamrune.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.EXTERNAL_EFFECT;
import static org.streamrune.core.projection.ProjectionDeliveryMode.TRANSACTIONAL_LOCAL;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.integration.ProjectionStoreRouting.Registration;
import org.streamrune.runtime.CacheAwareProjection;
import org.streamrune.test.InMemoryProjectionRepository;

class ProjectionStoreRoutingTest {

  /**
   * A processor with chosen capabilities and no behaviour: routing reads capabilities only, never
   * commits.
   */
  private record Capabilities(boolean transactional, boolean fencesCheckpoint)
      implements AtomicBatchProcessor {
    @Override
    public void executeAtomically(
        ProjectionName projectionName,
        List<EventEnvelope> batch,
        GlobalOffset newOffset,
        long fencingEpoch,
        ProjectionUpdater projectionUpdater,
        OffsetStore offsetStore) {
      throw new UnsupportedOperationException("routing never commits");
    }
  }

  /** Anything but NOOP is real leadership. A stub: this module has no Mockito. */
  private static final class RealLeadership implements SubscriptionLeadership {
    @Override
    public Optional<Lease> tryAcquire(SubscriptionName consumerName) {
      return Optional.of(new Lease(1L));
    }

    @Override
    public Optional<Lease> current(SubscriptionName consumerName) {
      return Optional.of(new Lease(1L));
    }

    @Override
    public void resign(SubscriptionName consumerName) {}

    @Override
    public void close() {}
  }

  private static final SubscriptionLeadership REAL = new RealLeadership();
  private static final SubscriptionLeadership NOOP = SubscriptionLeadership.NOOP;

  /** The event store's pair: fences its checkpoint, not transactional (no read models). */
  private static final CheckpointTarget SUBSCRIPTION =
      new CheckpointTarget(
          "subscription_checkpoint",
          new InMemoryProjectionRepository(),
          new Capabilities(false, true));

  /** The same pair over a processor that cannot fence: legal only under NOOP. */
  private static final CheckpointTarget UNFENCED_SUBSCRIPTION =
      new CheckpointTarget(
          "subscription_checkpoint",
          new InMemoryProjectionRepository(),
          AtomicBatchProcessor.nonAtomicAtLeastOnce());

  /** Transactional and fencing: the InMemoryProjectionRepository is repository and processor. */
  private static ReadModelStore fullStore(String name) {
    var repository = new InMemoryProjectionRepository();
    return ReadModelStore.of(name, repository, repository, repository);
  }

  /** A store whose processor has the given capabilities (a search index, an embedded database). */
  private static ReadModelStore store(String name, boolean transactional, boolean fences) {
    var repository = new InMemoryProjectionRepository();
    return ReadModelStore.of(name, repository, repository, new Capabilities(transactional, fences));
  }

  private static Projection noTarget() {
    return batch -> {};
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

  /** A JDK proxy forwarding every call to {@code target}, as a bean container's proxy does. */
  @SuppressWarnings("unchecked")
  private static <T> T forwardingProxy(Class<T> type, T target) {
    InvocationHandler forward =
        (proxy, method, args) -> {
          try {
            return method.invoke(target, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
        };
    return (T)
        Proxy.newProxyInstance(
            ProjectionStoreRoutingTest.class.getClassLoader(), new Class<?>[] {type}, forward);
  }

  private static Registration registration(
      String name, ProjectionDeliveryMode mode, String readModelStore, Projection projection) {
    return new Registration(name, mode, readModelStore, projection);
  }

  // ---------- Rule 1: explicit declaration ----------

  @Test
  void explicitNameWins() {
    var postgres = fullStore("postgres");
    var search = store("search", false, true);
    var other = fullStore("other");
    var stores = List.of(postgres, search, other);

    // over rule 4: an at-least-once registration goes to the declared store, not the subscription
    assertThat(
            ProjectionStoreRouting.route(
                registration("audit", AT_LEAST_ONCE_IDEMPOTENT, "search", noTarget()),
                stores,
                SUBSCRIPTION,
                REAL))
        .isEqualTo(search.checkpointTarget());
    // over rule 3: two transactional stores would be ambiguous without the declaration
    assertThat(
            ProjectionStoreRouting.route(
                registration("orders", TRANSACTIONAL_LOCAL, "other", noTarget()),
                stores,
                SUBSCRIPTION,
                REAL))
        .isEqualTo(other.checkpointTarget());
    // agreeing with writeTarget() is accepted
    assertThat(
            ProjectionStoreRouting.route(
                registration(
                    "orders", TRANSACTIONAL_LOCAL, "postgres", writingTo(postgres.repository())),
                stores,
                SUBSCRIPTION,
                REAL))
        .isEqualTo(postgres.checkpointTarget());
  }

  @Test
  void unknownExplicitNameFails() {
    var stores = List.of(fullStore("postgres"), store("search", false, true));
    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.route(
                    registration("orders", TRANSACTIONAL_LOCAL, "nope", noTarget()),
                    stores,
                    SUBSCRIPTION,
                    NOOP))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("projection 'orders'")
        .hasMessageContaining("TRANSACTIONAL_LOCAL")
        .hasMessageContaining("readModelStore = \"nope\"")
        .hasMessageContaining("'postgres'")
        .hasMessageContaining("'search'")
        .hasMessageContaining("readModelStore = \"<name>\"")
        .hasMessageContaining("writeTarget()");
  }

  @Test
  void explicitNameContradictingWriteTargetFails() {
    var postgres = fullStore("postgres");
    var search = store("search", false, true);
    var stores = List.of(postgres, search);
    // writeTarget() is another store's repository
    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.route(
                    registration(
                        "audit",
                        AT_LEAST_ONCE_IDEMPOTENT,
                        "search",
                        writingTo(postgres.repository())),
                    stores,
                    SUBSCRIPTION,
                    NOOP))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("projection 'audit'")
        .hasMessageContaining("AT_LEAST_ONCE_IDEMPOTENT")
        .hasMessageContaining("readModelStore = \"search\"")
        .hasMessageContaining("store 'postgres'")
        .hasMessageContaining("'search'")
        .hasMessageContaining("readModelStore = \"<name>\"")
        .hasMessageContaining("writeTarget()");
    // writeTarget() belongs to no store at all
    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.route(
                    registration(
                        "audit",
                        AT_LEAST_ONCE_IDEMPOTENT,
                        "search",
                        writingTo(new InMemoryProjectionRepository())),
                    stores,
                    SUBSCRIPTION,
                    NOOP))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("projection 'audit'")
        .hasMessageContaining("belongs to no read-model store")
        .hasMessageContaining("'postgres'")
        .hasMessageContaining("'search'");
  }

  // ---------- Rule 2: writeTarget() matches a store ----------

  @Test
  void writeTargetMatchRoutes() {
    var postgres = fullStore("postgres");
    var search = store("search", false, true);
    var stores = List.of(postgres, search);
    assertThat(
            ProjectionStoreRouting.route(
                registration("audit", AT_LEAST_ONCE_IDEMPOTENT, "", writingTo(search.repository())),
                stores,
                SUBSCRIPTION,
                REAL))
        .isEqualTo(search.checkpointTarget());
    assertThat(
            ProjectionStoreRouting.route(
                registration("orders", TRANSACTIONAL_LOCAL, "", writingTo(postgres.repository())),
                stores,
                SUBSCRIPTION,
                REAL))
        .isEqualTo(postgres.checkpointTarget());
    // rule 2 wins over rule 3's "the one transactional store" even when another one exists
    var second = fullStore("second");
    assertThat(
            ProjectionStoreRouting.route(
                registration("fx", EXTERNAL_EFFECT, "", writingTo(second.repository())),
                List.of(postgres, search, second),
                SUBSCRIPTION,
                REAL))
        .isEqualTo(second.checkpointTarget());
  }

  /**
   * Review Focus 2: decorators and bean proxies forward {@code writeTarget()} and {@code
   * writeTargetIdentity()}, so routing sees through them on either side.
   */
  @Test
  void decoratedProjectionRoutesToItsStore() {
    var postgres = fullStore("postgres");
    var search = store("search", false, true);
    var stores = List.of(postgres, search);
    Projection raw = writingTo(search.repository());

    Projection cacheAware = new CacheAwareProjection(raw, batch -> {});
    assertThat(
            ProjectionStoreRouting.route(
                registration("audit", AT_LEAST_ONCE_IDEMPOTENT, "", cacheAware),
                stores,
                SUBSCRIPTION,
                REAL))
        .isEqualTo(search.checkpointTarget());

    Projection proxied = forwardingProxy(Projection.class, raw);
    assertThat(
            ProjectionStoreRouting.route(
                registration("audit", AT_LEAST_ONCE_IDEMPOTENT, "", proxied),
                stores,
                SUBSCRIPTION,
                REAL))
        .isEqualTo(search.checkpointTarget());

    // the store's repository is the proxy (a bean proxy), the projection holds the real object
    var real = new InMemoryProjectionRepository();
    var proxiedStore =
        ReadModelStore.of("proxied", forwardingProxy(ProjectionRepository.class, real), real, real);
    assertThat(
            ProjectionStoreRouting.route(
                registration("orders", TRANSACTIONAL_LOCAL, "", writingTo(real)),
                List.of(postgres, proxiedStore),
                SUBSCRIPTION,
                REAL))
        .isEqualTo(proxiedStore.checkpointTarget());
  }

  // ---------- Rule 3: transactional mode, no match ----------

  @Test
  void transactionalWithoutTargetAndOneTransactionalStoreRoutesThere() {
    var postgres = fullStore("postgres");
    var stores = List.of(store("search", false, true), postgres);
    for (var mode : List.of(TRANSACTIONAL_LOCAL, EXTERNAL_EFFECT)) {
      assertThat(
              ProjectionStoreRouting.route(
                  registration("orders", mode, "", noTarget()), stores, SUBSCRIPTION, REAL))
          .isEqualTo(postgres.checkpointTarget());
    }
  }

  @Test
  void transactionalWithoutTargetAndTwoTransactionalStoresFailsNamingBoth() {
    var stores = List.of(fullStore("postgres"), store("search", false, true), fullStore("other"));
    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.route(
                    registration("orders", EXTERNAL_EFFECT, "", noTarget()),
                    stores,
                    SUBSCRIPTION,
                    NOOP))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("projection 'orders'")
        .hasMessageContaining("EXTERNAL_EFFECT")
        .hasMessageContaining("'postgres'")
        .hasMessageContaining("'other'")
        .hasMessageContaining("'search'")
        .hasMessageContaining("readModelStore = \"<name>\"")
        .hasMessageContaining("writeTarget()");
  }

  @Test
  void transactionalWithoutTargetAndNoTransactionalStoreFails() {
    for (var stores : List.of(List.<ReadModelStore>of(), List.of(store("search", false, true)))) {
      assertThatThrownBy(
              () ->
                  ProjectionStoreRouting.route(
                      registration("orders", TRANSACTIONAL_LOCAL, "", noTarget()),
                      stores,
                      SUBSCRIPTION,
                      NOOP))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("projection 'orders'")
          .hasMessageContaining("TRANSACTIONAL_LOCAL")
          .hasMessageContaining("no read-model store is transactional")
          .hasMessageContaining("readModelStore = \"<name>\"")
          .hasMessageContaining("writeTarget()")
          .hasMessageContaining("AT_LEAST_ONCE_IDEMPOTENT");
    }
    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.route(
                    registration("orders", TRANSACTIONAL_LOCAL, "", noTarget()),
                    List.of(),
                    SUBSCRIPTION,
                    NOOP))
        .hasMessageContaining("no read-model store is defined");
  }

  @Test
  void transactionalWithUnmatchedTargetFails() {
    var stores = List.of(fullStore("postgres"), store("search", false, true));
    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.route(
                    registration(
                        "orders",
                        TRANSACTIONAL_LOCAL,
                        "",
                        writingTo(new InMemoryProjectionRepository())),
                    stores,
                    SUBSCRIPTION,
                    NOOP))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("projection 'orders'")
        .hasMessageContaining("TRANSACTIONAL_LOCAL")
        .hasMessageContaining("belongs to no read-model store")
        .hasMessageContaining("'postgres'")
        .hasMessageContaining("'search'")
        .hasMessageContaining("readModelStore = \"<name>\"")
        .hasMessageContaining("writeTarget()");
  }

  // ---------- Rule 4: at-least-once, no match ----------

  @Test
  void atLeastOnceWithoutMatchUsesSubscriptionCheckpoint() {
    for (var stores : List.of(List.<ReadModelStore>of(), List.of(fullStore("postgres")))) {
      assertThat(
              ProjectionStoreRouting.route(
                  registration("audit", AT_LEAST_ONCE_IDEMPOTENT, "", noTarget()),
                  stores,
                  SUBSCRIPTION,
                  REAL))
          .isSameAs(SUBSCRIPTION);
    }
  }

  @Test
  void atLeastOnceWithForeignRepositoryUsesSubscriptionCheckpoint() {
    assertThat(
            ProjectionStoreRouting.route(
                registration(
                    "audit",
                    AT_LEAST_ONCE_IDEMPOTENT,
                    "",
                    writingTo(new InMemoryProjectionRepository())),
                List.of(fullStore("postgres"), store("search", false, true)),
                SUBSCRIPTION,
                REAL))
        .isSameAs(SUBSCRIPTION);
  }

  // ---------- Per-registration checks ----------

  @Test
  void transactionalModeOnNonTransactionalStoreFails() {
    var search = store("search", false, true);
    var stores = List.of(fullStore("postgres"), search);
    for (var projection : List.of(noTarget(), writingTo(search.repository()))) {
      String declared = projection.writeTarget().isPresent() ? "" : "search";
      assertThatThrownBy(
              () ->
                  ProjectionStoreRouting.route(
                      registration("orders", TRANSACTIONAL_LOCAL, declared, projection),
                      stores,
                      SUBSCRIPTION,
                      NOOP))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("projection 'orders'")
          .hasMessageContaining("TRANSACTIONAL_LOCAL")
          .hasMessageContaining("routed to 'search'")
          .hasMessageContaining("transactional()")
          .hasMessageContaining("'postgres'")
          .hasMessageContaining("readModelStore = \"<name>\"")
          .hasMessageContaining("writeTarget()");
    }
  }

  @Test
  void realLeadershipOnNonFencingTargetFails() {
    // an at-least-once registration on an unfenced subscription checkpoint
    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.route(
                    registration("audit", AT_LEAST_ONCE_IDEMPOTENT, "", noTarget()),
                    List.of(fullStore("postgres")),
                    UNFENCED_SUBSCRIPTION,
                    REAL))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("projection 'audit'")
        .hasMessageContaining("AT_LEAST_ONCE_IDEMPOTENT")
        .hasMessageContaining("routed to 'subscription_checkpoint'")
        .hasMessageContaining("fencesCheckpoint()")
        .hasMessageContaining(RealLeadership.class.getName())
        .hasMessageContaining("'postgres'")
        .hasMessageContaining("readModelStore = \"<name>\"")
        .hasMessageContaining("writeTarget()")
        .hasMessageContaining("streamrune.subscription.single-active-consumer.enabled=false");
    // a transactional registration on a transactional, non-fencing store (embedded database)
    var stores = List.of(store("sqlite", true, false), fullStore("postgres"));
    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.route(
                    registration("orders", TRANSACTIONAL_LOCAL, "sqlite", noTarget()),
                    stores,
                    SUBSCRIPTION,
                    REAL))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("projection 'orders'")
        .hasMessageContaining("TRANSACTIONAL_LOCAL")
        .hasMessageContaining("routed to 'sqlite'")
        .hasMessageContaining("fencesCheckpoint()")
        .hasMessageContaining("'postgres'");
  }

  @Test
  void noopLeadershipAcceptsNonFencingTarget() {
    assertThat(
            ProjectionStoreRouting.route(
                registration("audit", AT_LEAST_ONCE_IDEMPOTENT, "", noTarget()),
                List.of(fullStore("postgres")),
                UNFENCED_SUBSCRIPTION,
                NOOP))
        .isSameAs(UNFENCED_SUBSCRIPTION);
    var sqlite = store("sqlite", true, false);
    assertThat(
            ProjectionStoreRouting.route(
                registration("orders", TRANSACTIONAL_LOCAL, "", noTarget()),
                List.of(sqlite),
                SUBSCRIPTION,
                NOOP))
        .isEqualTo(sqlite.checkpointTarget());
  }

  /** Ported: real leadership is satisfied by a fencing target, whatever the mode. */
  @Test
  void realLeadershipAcceptsFencingTarget() {
    var postgres = fullStore("postgres");
    assertThat(
            ProjectionStoreRouting.route(
                registration("audit", AT_LEAST_ONCE_IDEMPOTENT, "", noTarget()),
                List.of(postgres),
                SUBSCRIPTION,
                REAL))
        .isSameAs(SUBSCRIPTION);
    assertThat(
            ProjectionStoreRouting.route(
                registration("orders", TRANSACTIONAL_LOCAL, "", noTarget()),
                List.of(postgres),
                SUBSCRIPTION,
                REAL))
        .isEqualTo(postgres.checkpointTarget());
  }

  // ---------- Store set validation ----------

  @Test
  void duplicateStoreNameFails() {
    var first = fullStore("postgres");
    var second = store("postgres", false, true);
    assertThatThrownBy(() -> ProjectionStoreRouting.validateStores(List.of(first, second)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("'postgres'")
        .hasMessageContaining(first.getClass().getName())
        .hasMessageContaining(InMemoryProjectionRepository.class.getName());
    // route refuses the same set: rule 2 must never see an ambiguous store set
    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.route(
                    registration("audit", AT_LEAST_ONCE_IDEMPOTENT, "", noTarget()),
                    List.of(first, second),
                    SUBSCRIPTION,
                    NOOP))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("'postgres'");
  }

  @Test
  void duplicateWriteTargetIdentityFails() {
    var repository = new InMemoryProjectionRepository();
    var a = ReadModelStore.of("a", repository, repository, repository);
    // a second store over a bean proxy of the same repository: equal identity, different object
    var b =
        ReadModelStore.of(
            "b", forwardingProxy(ProjectionRepository.class, repository), repository, repository);
    assertThatThrownBy(() -> ProjectionStoreRouting.validateStores(List.of(a, b)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("'a'")
        .hasMessageContaining("'b'")
        .hasMessageContaining("writeTargetIdentity()");
  }

  @Test
  void distinctStoresAreValid() {
    ProjectionStoreRouting.validateStores(List.of());
    ProjectionStoreRouting.validateStores(List.of(fullStore("postgres"), fullStore("search")));
  }

  // ---------- Arguments ----------

  @Test
  void requiredArgumentsAreRefused() {
    var stores = List.of(fullStore("postgres"));
    var ok = registration("audit", AT_LEAST_ONCE_IDEMPOTENT, "", noTarget());
    assertThatThrownBy(() -> ProjectionStoreRouting.route(ok, stores, SUBSCRIPTION, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("leadership is required");
    assertThatThrownBy(() -> ProjectionStoreRouting.route(null, stores, SUBSCRIPTION, NOOP))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("registration is required");
    assertThatThrownBy(() -> ProjectionStoreRouting.route(ok, null, SUBSCRIPTION, NOOP))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("stores is required");
    assertThatThrownBy(() -> ProjectionStoreRouting.route(ok, stores, null, NOOP))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("subscriptionCheckpoint is required");
    assertThatThrownBy(() -> registration(null, AT_LEAST_ONCE_IDEMPOTENT, "", noTarget()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("projectionName is required");
    assertThatThrownBy(() -> registration("a", null, "", noTarget()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("mode is required");
    assertThatThrownBy(() -> registration("a", AT_LEAST_ONCE_IDEMPOTENT, null, noTarget()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("readModelStore is required (\"\" for none)");
    assertThatThrownBy(() -> registration("a", AT_LEAST_ONCE_IDEMPOTENT, "", null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("projection is required");
  }

  @Test
  void projectionAndStoreNamesAreSanitized() {
    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.route(
                    registration("bad\nname", TRANSACTIONAL_LOCAL, "no\npe", noTarget()),
                    List.of(fullStore("evil\nstore")),
                    SUBSCRIPTION,
                    NOOP))
        .satisfies(ex -> assertThat(ex.getMessage()).doesNotContain("\n"));
  }

  // ---------- INLINE ----------

  @Test
  void inlineRequiresAtLeastOnce() {
    ProjectionStoreRouting.requireInlineMode("x", AT_LEAST_ONCE_IDEMPOTENT);
    for (var mode : List.of(TRANSACTIONAL_LOCAL, EXTERNAL_EFFECT)) {
      assertThatThrownBy(() -> ProjectionStoreRouting.requireInlineMode("x", mode))
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
  void inlineProjectionNamesAreSanitized() {
    assertThatThrownBy(
            () -> ProjectionStoreRouting.requireInlineMode("bad\nname", TRANSACTIONAL_LOCAL))
        .satisfies(ex -> assertThat(ex.getMessage()).doesNotContain("\n"));
  }

  @Test
  void inlineInfoIsTheOneSentence() {
    assertThat(ProjectionStoreRouting.INLINE_INFO)
        .contains("'{}'")
        .contains("AT_LEAST_ONCE_IDEMPOTENT inline");
  }

  // ── selecting the stores: the default replaced only by name ─────────────────────

  /** A default-store supplier that counts its calls: calling it is what creates the store. */
  private static final class DefaultSupplier
      implements java.util.function.Supplier<Optional<ReadModelStore>> {
    private final Optional<ReadModelStore> store;
    int calls;

    DefaultSupplier(Optional<ReadModelStore> store) {
      this.store = store;
    }

    @Override
    public Optional<ReadModelStore> get() {
      calls++;
      return store;
    }
  }

  @Test
  void selectStores_anApplicationStoreNamedPostgresReplacesTheDefault_whichIsNeverCreated() {
    var defaultStore = new DefaultSupplier(Optional.of(fullStore("postgres")));
    var replacement = fullStore("postgres");
    var search = fullStore("search");

    assertThat(ProjectionStoreRouting.selectStores(defaultStore, List.of(search, replacement)))
        .containsExactly(search, replacement);
    assertThat(defaultStore.calls).as("the replaced default is never created").isZero();
  }

  @Test
  void selectStores_anApplicationStoreOfAnotherNameKeepsTheDefault_first() {
    var postgres = fullStore("postgres");
    var defaultStore = new DefaultSupplier(Optional.of(postgres));
    var search = fullStore("search");

    assertThat(ProjectionStoreRouting.selectStores(defaultStore, List.of(search)))
        .containsExactly(postgres, search);
    assertThat(defaultStore.calls).isEqualTo(1);
  }

  @Test
  void selectStores_withoutADefault_isTheApplicationsStores() {
    var search = fullStore("search");

    assertThat(
            ProjectionStoreRouting.selectStores(
                new DefaultSupplier(Optional.empty()), List.of(search)))
        .containsExactly(search);
    assertThat(ProjectionStoreRouting.selectStores(Optional::empty, List.of())).isEmpty();
  }

  @Test
  void selectStores_duplicateApplicationStoreNames_failBeforeTheDefaultIsCreated() {
    var defaultStore = new DefaultSupplier(Optional.of(fullStore("postgres")));

    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.selectStores(
                    defaultStore, List.of(fullStore("search"), store("search", false, true))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Two read-model stores are named 'search'");
    assertThat(defaultStore.calls).isZero();
  }

  @Test
  void selectStores_twoApplicationStoresNamedPostgres_fail() {
    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.selectStores(
                    Optional::empty, List.of(fullStore("postgres"), fullStore("postgres"))))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Two read-model stores are named 'postgres'");
  }

  @Test
  void selectStores_anApplicationStoreOverTheDefaultsRepository_fails() {
    var repository = new InMemoryProjectionRepository();
    var postgres = ReadModelStore.of("postgres", repository, repository, repository);
    var alias = ReadModelStore.of("alias", repository, repository, repository);

    assertThatThrownBy(
            () -> ProjectionStoreRouting.selectStores(() -> Optional.of(postgres), List.of(alias)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("'postgres' and 'alias' write to the same place");
  }

  @Test
  void selectStores_aDefaultNotNamedPostgres_isRefused() {
    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.selectStores(
                    () -> Optional.of(fullStore("other")), List.of()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("The default read-model store must be named 'postgres'")
        .hasMessageContaining("'other'");
  }

  @Test
  void selectStores_refusesNulls() {
    assertThatThrownBy(() -> ProjectionStoreRouting.selectStores(null, List.of()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("defaultStore is required");
    assertThatThrownBy(() -> ProjectionStoreRouting.selectStores(Optional::empty, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("applicationStores is required");
    assertThatThrownBy(() -> ProjectionStoreRouting.selectStores(() -> null, List.of()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("defaultStore must supply an Optional");
    var withNull = new java.util.ArrayList<ReadModelStore>();
    withNull.add(null);
    assertThatThrownBy(() -> ProjectionStoreRouting.selectStores(Optional::empty, withNull))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("stores must not hold null");
  }

  @Test
  void theDefaultStoreNameAndBeanName() {
    assertThat(ProjectionStoreRouting.DEFAULT_STORE).isEqualTo("postgres");
    assertThat(ProjectionStoreRouting.DEFAULT_STORE_BEAN)
        .isEqualTo("streamRuneDefaultReadModelStore");
  }

  // ── the default store's repository ──────────────────────────────────────────────

  @Test
  void defaultRepository_isTheRepositoryOfTheStoreWithThatName() {
    ReadModelStore search = fullStore("search");
    ReadModelStore postgres = fullStore("postgres");

    assertThat(ProjectionStoreRouting.defaultRepository(List.of(search, postgres), "postgres"))
        .isSameAs(postgres.repository());
  }

  @Test
  void defaultRepository_withoutAStoreOfThatName_failsNamingTheStoresAndTheFix() {
    assertThatThrownBy(
            () ->
                ProjectionStoreRouting.defaultRepository(
                    List.of(fullStore("search"), fullStore("memory")), "postgres"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "No read-model store is named 'postgres' (stores: 'search', 'memory')")
        .hasMessageContaining("Define a ProjectionRepository bean, or name one store 'postgres'");
  }

  @Test
  void defaultRepository_withNoStore_failsSayingNoneIsDefined() {
    assertThatThrownBy(() -> ProjectionStoreRouting.defaultRepository(List.of(), "postgres"))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "No read-model store is named 'postgres' (no read-model store is defined)")
        .hasMessageContaining("Define a ProjectionRepository bean, or name one store 'postgres'");
  }
}
