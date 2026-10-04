package org.streamrune.core.projection;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.LockMode;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Versioned;
import org.streamrune.core.types.ProjectionName;

class BaseProjectionTest {

  static class RecordingRepository implements ProjectionRepository {
    final Map<String, Object> store = new HashMap<>();
    final Map<String, Long> versions = new HashMap<>();

    @Override
    public <T> void save(ProjectionName projectionName, String id, T readModel) {
      store.put(projectionName.value() + ":" + id, readModel);
    }

    @Override
    public <T> Optional<T> findById(ProjectionName projectionName, String id, Class<T> type) {
      Object v = store.get(projectionName.value() + ":" + id);
      return Optional.ofNullable(type.cast(v));
    }

    @Override
    public <T> List<T> findAll(ProjectionName projectionName, Class<T> type) {
      return List.of();
    }

    @Override
    public void delete(ProjectionName projectionName, String id) {
      store.remove(projectionName.value() + ":" + id);
    }

    @Override
    public <T> Page<T> findAll(
        ProjectionName projectionName, Class<T> type, PageRequest pageRequest) {
      return new Page<>(List.of(), 0, 0, pageRequest.size());
    }

    @Override
    public <T> Optional<Versioned<T>> findById(
        ProjectionName projectionName, String id, Class<T> type, LockMode lockMode) {
      return Optional.empty();
    }

    @Override
    public <T> void save(
        ProjectionName projectionName, String id, T readModel, long expectedVersion) {
      String key = projectionName.value() + ":" + id;
      long current = versions.getOrDefault(key, 0L);
      if (current != expectedVersion) {
        throw new OptimisticLockException(
            "Optimistic lock conflict on read model '%s': expected version %d but was %d"
                .formatted(key, expectedVersion, current));
      }
      versions.put(key, current + 1);
      store.put(key, readModel);
    }
  }

  static class CartProjection extends BaseProjection {
    CartProjection(ProjectionRepository repo) {
      super(repo);
    }

    @Override
    public void process(List<EventEnvelope> batch) {}

    void doSave(String id, String readModel) {
      save(id, readModel);
    }

    Optional<String> doFind(String id) {
      return findById(id, String.class);
    }

    ProjectionName exposeProjectionName() {
      return projectionName();
    }
  }

  static class CartViewProjection extends BaseProjection {
    CartViewProjection(ProjectionRepository repo) {
      super(repo);
    }

    @Override
    public void process(List<EventEnvelope> batch) {}

    ProjectionName exposeProjectionName() {
      return projectionName();
    }
  }

  /**
   * Binds its transaction-scoped repository, then holds the binding live (via {@code bound}) until
   * BOTH threads have bound theirs, saves under the current thread's name, and holds again (via
   * {@code saved}) until both have saved — so a plain-field binding is observably overwritten by
   * whichever thread bound last, routing both saves into the same (wrong) repository.
   */
  static final class BarrierProjection extends BaseProjection {
    private final CyclicBarrier bound;
    private final CyclicBarrier saved;

    BarrierProjection(ProjectionRepository fallback, CyclicBarrier bound, CyclicBarrier saved) {
      super(fallback, "cross_write");
      this.bound = bound;
      this.saved = saved;
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      try {
        bound.await(5, TimeUnit.SECONDS); // both threads have now bound their repo
        save(Thread.currentThread().getName(), "v"); // routes through the bound repo
        saved.await(5, TimeUnit.SECONDS); // hold both bindings live until both have saved
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  void concurrentProcess_bindsRepositoryPerThread_noCrossWrite() throws Exception {
    // The transaction-scoped repository was a plain non-volatile field, so a
    // concurrent DLQ replay (or the javadoc-recommended inline+runner dual registration) on the
    // SAME BaseProjection instance raced the binding — a save() could route into the OTHER thread's
    // open transaction (silent lost write / JDBC corruption). Confining the binding to a
    // ThreadLocal
    // makes each thread's save() hit its OWN bound repository. Both threads bind before either
    // saves; with a plain field both saves would land in whichever repo was bound last.
    var fallback = new RecordingRepository();
    var repoA = new RecordingRepository();
    var repoB = new RecordingRepository();
    var projection = new BarrierProjection(fallback, new CyclicBarrier(2), new CyclicBarrier(2));

    Thread tA = new Thread(() -> projection.process(List.of(), repoA), "cross-A");
    Thread tB = new Thread(() -> projection.process(List.of(), repoB), "cross-B");
    tA.start();
    tB.start();
    tA.join(TimeUnit.SECONDS.toMillis(10));
    tB.join(TimeUnit.SECONDS.toMillis(10));
    assertFalse(tA.isAlive() || tB.isAlive(), "both threads must complete");

    // Each thread's save() must have routed to ITS OWN bound repository — no cross-write.
    assertEquals(
        Set.of("cross_write:cross-A"),
        repoA.store.keySet(),
        "thread A's save must land in repoA only");
    assertEquals(
        Set.of("cross_write:cross-B"),
        repoB.store.keySet(),
        "thread B's save must land in repoB only");
    assertTrue(fallback.store.isEmpty(), "no write should fall back to the captured repository");
  }

  @Test
  void saveAndFindRoundTrip() {
    var repo = new RecordingRepository();
    var p = new CartProjection(repo);

    p.doSave("cart-1", "view-data");
    assertEquals(Optional.of("view-data"), p.doFind("cart-1"));
    assertEquals(Optional.empty(), p.doFind("missing"));
  }

  /**
   * Writes and reads back through the inherited {@code save()/findById()} helpers, so its routing
   * follows the repository bound by {@link BaseProjection#process(List, ProjectionRepository)}.
   */
  static class AccumulatingProjection extends BaseProjection {
    AccumulatingProjection(ProjectionRepository repo) {
      super(repo, "Cart");
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      int current = findById("count", Integer.class).orElse(0);
      save("count", current + 1);
    }
  }

  @Test
  void processRoutesWritesThroughTheSuppliedTransactionScopedRepository() {
    // When the runner passes a transaction-scoped repository via the 2-arg process(), the
    // inherited save()/findById() helpers must write through IT, not the captured repository, so
    // the read-model write is atomic with the offset checkpoint.
    var captured = new RecordingRepository();
    var txScoped = new RecordingRepository();
    var p = new AccumulatingProjection(captured);

    p.process(List.of(), txScoped);

    assertEquals(1, txScoped.store.get("Cart:count"), "write went to the transaction-scoped repo");
    assertNull(captured.store.get("Cart:count"), "captured repo must be untouched");
  }

  @Test
  void processFallsBackToTheCapturedRepositoryWhenNoTransactionScopedRepositoryIsSupplied() {
    // Non-transactional processor (or a direct process(batch) call) passes null: the helpers keep
    // writing through the captured repository, preserving the previous at-least-once behaviour.
    var captured = new RecordingRepository();
    var p = new AccumulatingProjection(captured);

    p.process(List.of(), null);
    assertEquals(1, captured.store.get("Cart:count"));

    p.process(List.of()); // direct one-arg call also uses the captured repository
    assertEquals(2, captured.store.get("Cart:count"));
  }

  @Test
  void processRestoresThePreviousBindingAfterEachCall() {
    // Re-entrancy: the binding is saved/restored so a nested-then-outer sequence never leaks the
    // inner repository into a later plain process(batch) call.
    var captured = new RecordingRepository();
    var txScoped = new RecordingRepository();
    var p = new AccumulatingProjection(captured);

    p.process(List.of(), txScoped);
    p.process(List.of()); // must fall back to captured now that the tx binding is cleared

    assertEquals(1, txScoped.store.get("Cart:count"));
    assertEquals(1, captured.store.get("Cart:count"));
  }

  /** Deletes the read model named by the batch through the inherited {@code delete()} helper. */
  static class DeletingProjection extends BaseProjection {
    DeletingProjection(ProjectionRepository repo) {
      super(repo, "Cart");
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      delete("c-1");
    }
  }

  @Test
  void deleteRoutesThroughTheSuppliedTransactionScopedRepository() {
    // A delete inside process() belongs to the batch's transaction like a save: through the
    // captured repository it would run outside it and miss a row the same batch staged.
    var captured = new RecordingRepository();
    var txScoped = new RecordingRepository();
    captured.save(ProjectionName.of("Cart"), "c-1", "committed");
    txScoped.save(ProjectionName.of("Cart"), "c-1", "staged");
    var p = new DeletingProjection(captured);

    p.process(List.of(), txScoped);

    assertNull(txScoped.store.get("Cart:c-1"), "the delete went to the transaction-scoped repo");
    assertEquals("committed", captured.store.get("Cart:c-1"), "captured repo must be untouched");
  }

  @Test
  void deleteFallsBackToTheCapturedRepositoryWhenNoTransactionScopedRepositoryIsSupplied() {
    var captured = new RecordingRepository();
    captured.save(ProjectionName.of("Cart"), "c-1", "row");
    var p = new DeletingProjection(captured);

    p.process(List.of(), null);

    assertNull(captured.store.get("Cart:c-1"));
  }

  @Test
  void recordingRepositoryEnforcesTheOptimisticSaveContract() {
    var repo = new RecordingRepository();

    repo.save(ProjectionName.of("Cart"), "c1", "v0", 0L);
    assertThrows(
        OptimisticLockException.class,
        () -> repo.save(ProjectionName.of("Cart"), "c1", "stale", 0L));
    repo.save(ProjectionName.of("Cart"), "c1", "v1", 1L);
    assertEquals(Optional.of("v1"), repo.findById(ProjectionName.of("Cart"), "c1", String.class));
  }

  static class ExplicitNameProjection extends BaseProjection {
    ExplicitNameProjection(ProjectionRepository repo, String name) {
      super(repo, name);
    }

    @Override
    public void process(List<EventEnvelope> batch) {}

    ProjectionName exposeProjectionName() {
      return projectionName();
    }
  }

  @Test
  void projectionNameStripsProjectionSuffix() {
    var repo = new RecordingRepository();
    assertEquals(ProjectionName.of("cart"), new CartProjection(repo).exposeProjectionName());
    assertEquals(
        ProjectionName.of("cart_view"), new CartViewProjection(repo).exposeProjectionName());
  }

  /**
   * The derived name is snake_case, so it is a valid read-model name for every repository ({@code
   * [a-z_][a-z0-9_]{0,57}}): {@code CartViewProjection} saves under {@code cart_view}, not {@code
   * CartView}, which JdbcProjectionRepository rejects on the first write.
   */
  @Test
  void derivedNameIsSnakeCase() {
    assertEquals("order", BaseProjection.deriveProjectionName("Order"));
    assertEquals("cart_view", BaseProjection.deriveProjectionName("CartView"));
    assertEquals("order_summary", BaseProjection.deriveProjectionName("OrderSummary"));
    assertEquals("http_request_log", BaseProjection.deriveProjectionName("HTTPRequestLog"));
    assertEquals("cart2_view", BaseProjection.deriveProjectionName("Cart2View"));
    assertEquals("cart_view", BaseProjection.deriveProjectionName("Cart_View"));
    assertEquals("sku", BaseProjection.deriveProjectionName("SKU"));
  }

  /** A class name that has no valid snake_case form fails in the constructor, not on a write. */
  @Test
  void aDerivedNameOutsideTheRuleIsRejectedInTheConstructor() {
    for (String simpleName : List.of("ÚčetView", "Cart$View", "A" + "b".repeat(58))) {
      var ex =
          assertThrows(
              IllegalStateException.class,
              () -> BaseProjection.deriveProjectionName(simpleName),
              simpleName);
      assertTrue(ex.getMessage().contains("explicit name"), ex.getMessage());
    }
  }

  @Test
  void explicitNameOverridesDerivedName() {
    var repo = new RecordingRepository();
    assertEquals(
        ProjectionName.of("orders-by-customer"),
        new ExplicitNameProjection(repo, "orders-by-customer").exposeProjectionName());
  }

  @Test
  void explicitBlankNameIsRejected() {
    var repo = new RecordingRepository();
    assertThrows(IllegalArgumentException.class, () -> new ExplicitNameProjection(repo, " "));
    assertThrows(IllegalArgumentException.class, () -> new ExplicitNameProjection(repo, null));
  }

  @Test
  void underivableNameIsRejected() {
    var repo = new RecordingRepository();
    // Anonymous class has an empty simple name — no projection name can be derived.
    assertThrows(
        IllegalStateException.class,
        () ->
            new BaseProjection(repo) {
              @Override
              public void process(List<EventEnvelope> batch) {}
            });
  }

  @Test
  void classNamedExactlyProjectionIsRejected() {
    var repo = new RecordingRepository();
    // Stripping the "Projection" suffix leaves an empty name — must fail, not register under "".
    class Projection extends BaseProjection {
      Projection() {
        super(repo);
      }

      @Override
      public void process(List<EventEnvelope> batch) {}
    }
    var ex = assertThrows(IllegalStateException.class, Projection::new);
    assertTrue(ex.getMessage().contains("explicit name"), ex.getMessage());
  }

  @Test
  void sameSimpleNameDerivesTheSameProjectionName() {
    // Documented hazard: name derivation uses the simple class name only, so two projection
    // classes sharing a simple name (e.g. across packages) silently share read models.
    var repo = new RecordingRepository();
    assertEquals(
        new CartProjection(repo).exposeProjectionName(),
        new Elsewhere.CartProjection(repo).exposeProjectionName());
  }

  /** Simulates a same-simple-name projection class from another package. */
  static class Elsewhere {
    static class CartProjection extends BaseProjection {
      CartProjection(ProjectionRepository repo) {
        super(repo);
      }

      @Override
      public void process(List<EventEnvelope> batch) {}

      ProjectionName exposeProjectionName() {
        return projectionName();
      }
    }
  }

  @Test
  void writeTarget_isTheCapturedRepository() {
    var repo = new RecordingRepository();
    var projection =
        new BaseProjection(repo, "orders") {
          @Override
          public void process(List<EventEnvelope> batch) {}
        };
    assertThat(projection.writeTarget()).containsSame(repo);
  }

  @Test
  void writeTarget_isEmptyWhenConstructedWithoutARepository() {
    var projection =
        new BaseProjection(null, "orders") {
          @Override
          public void process(List<EventEnvelope> batch) {}
        };
    assertThat(projection.writeTarget()).isEmpty();
  }
}
