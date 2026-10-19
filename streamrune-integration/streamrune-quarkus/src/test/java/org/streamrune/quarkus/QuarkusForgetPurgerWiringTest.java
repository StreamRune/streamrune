package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.arc.All;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Cacheable;
import org.streamrune.core.Query;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.SubjectId;
import org.streamrune.runtime.CachingQueryBus;
import org.streamrune.runtime.SimpleQueryBus;
import org.streamrune.runtime.gdpr.ForgetSubjectService;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * The auto-produced {@link ForgetSubjectService} must invoke every registered {@link
 * SubjectDataPurger}. Boots a real Arc container (via {@link RealArcTestContainer}) so the real
 * {@code @All List<SubjectDataPurger>} injection is exercised against a real purger bean — not one
 * hand-passed into the producer, which is how the missing collection slipped past the unit tests.
 * The {@code forgetSubjectService} bean delegates to the actual {@link StreamRuneProducers} method.
 */
class QuarkusForgetPurgerWiringTest {

  private static final StreamRuneProducers PRODUCERS = new StreamRuneProducers();

  @ApplicationScoped
  public static class GdprDefaults {
    @Produces
    @Singleton
    public CryptoEngine cryptoEngine() {
      return new InMemoryCryptoEngine();
    }

    @Produces
    @Singleton
    public ForgetSubjectService forgetSubjectService(
        Instance<CryptoEngine> crypto,
        Instance<AuditStore> audit,
        @All List<SubjectDataPurger> purgers,
        Instance<org.streamrune.core.StreamRuneMetrics> metrics,
        Instance<CachingQueryBus> queryCache) {
      // The snapshot purge is tested in aMissingSnapshotPurgerFailsStartup.
      return PRODUCERS.forgetSubjectService(
          crypto,
          audit,
          purgers,
          metrics,
          queryCache,
          TestProperties.of(Map.of("streamrune.gdpr.snapshot-purge", "none")));
    }
  }

  /** The defaults, bound the way Quarkus binds them: the snapshot purge stays {@code required}. */
  @ApplicationScoped
  public static class RequiredSnapshotPurgeDefaults {
    @Produces
    @Singleton
    public CryptoEngine cryptoEngine() {
      return new InMemoryCryptoEngine();
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return TestProperties.defaults();
    }

    @Produces
    @Singleton
    public ForgetSubjectService forgetSubjectService(
        Instance<CryptoEngine> crypto,
        Instance<AuditStore> audit,
        @All List<SubjectDataPurger> purgers,
        Instance<org.streamrune.core.StreamRuneMetrics> metrics,
        Instance<CachingQueryBus> queryCache,
        StreamRuneQuarkusProperties properties) {
      return PRODUCERS.forgetSubjectService(
          crypto, audit, purgers, metrics, queryCache, properties);
    }
  }

  /**
   * A crypto engine and no snapshot purger fail startup, not the first forget: the forget service
   * is a dependent bean the application may inject only into a request-time resource, so the config
   * validator builds it on the startup event.
   */
  @Test
  void aMissingSnapshotPurgerFailsStartup() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                RequiredSnapshotPurgeDefaults.class,
                RecordingPurger.class,
                StreamRuneConfigValidator.class,
                ForgetSubjectService.class,
                CryptoEngine.class,
                SubjectDataPurger.class))) {
      var failure =
          assertThrows(
              Exception.class,
              () ->
                  arc.container()
                      .beanManager()
                      .getEvent()
                      .select(StartupEvent.class)
                      .fire(new StartupEvent()));
      boolean found = false;
      for (Throwable t = failure; t != null && !found; t = t.getCause()) {
        found =
            t.getMessage() != null
                && t.getMessage()
                    .contains(
                        "The ForgetSubjectService StreamRune configures has no snapshot purger.");
      }
      assertTrue(found, "the startup failure says what to add: " + failure);
    }
  }

  /**
   * A snapshot purger produced the way an application commonly writes it: normal-scoped, typed by
   * the purger API. The forget receives Arc's client proxy, which implements only {@link
   * SubjectDataPurger}.
   */
  @ApplicationScoped
  public static class NormalScopedSnapshotPurgerProducer {
    @Produces
    @ApplicationScoped
    public SubjectDataPurger customerSnapshotPurger() {
      return new org.streamrune.postgres.PostgresSnapshotStorePurger(
          org.mockito.Mockito.mock(javax.sql.DataSource.class), subjectId -> List.of());
    }
  }

  /**
   * The marker is read on the instance behind the client proxy, so an {@code @ApplicationScoped}
   * producer typed {@link SubjectDataPurger} that returns a {@code PostgresSnapshotStorePurger}
   * counts as the snapshot purger: startup passes, and the forget runs it under its own name.
   */
  @Test
  void aNormalScopedProducerTypedSubjectDataPurgerCountsAsTheSnapshotPurger() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                RequiredSnapshotPurgeDefaults.class,
                NormalScopedSnapshotPurgerProducer.class,
                StreamRuneConfigValidator.class,
                ForgetSubjectService.class,
                CryptoEngine.class,
                SubjectDataPurger.class))) {
      arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());

      var result =
          arc.container()
              .instance(ForgetSubjectService.class)
              .get()
              .forget(SubjectId.of("subject-42"), null);

      assertEquals(
          List.of("snapshot_store"),
          result.purgeOutcomes().stream()
              .map(org.streamrune.runtime.gdpr.ForgetResult.PurgeOutcome::purgerName)
              .toList());
      assertTrue(result.fullyErased());
    }
  }

  /** The query cache {@code streamrune.query-cache.enabled=true} produces, for the test below. */
  @ApplicationScoped
  public static class QueryCacheDefaults {
    @Produces
    @Singleton
    public CachingQueryBus cachingQueryBus() {
      return CachingQueryBus.builder().delegate(new SimpleQueryBus()).build();
    }
  }

  @Cacheable(scope = Cacheable.Scope.GLOBAL, ttlSeconds = 3600)
  public record CustomerRowQuery(String customerId) implements Query<String> {}

  @Singleton
  public static class RecordingPurger implements SubjectDataPurger {
    static final List<SubjectId> PURGED = new CopyOnWriteArrayList<>();

    @Override
    public String name() {
      return "recording";
    }

    @Override
    public void purge(SubjectId subjectId) {
      PURGED.add(subjectId);
    }
  }

  @Test
  void forgetInvokesRegisteredPurger() throws Exception {
    RecordingPurger.PURGED.clear();
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                GdprDefaults.class,
                RecordingPurger.class,
                ForgetSubjectService.class,
                CryptoEngine.class,
                SubjectDataPurger.class))) {
      var forget = arc.container().instance(ForgetSubjectService.class);
      assertTrue(forget.isAvailable(), "the auto-produced ForgetSubjectService must resolve");
      forget.get().forget(SubjectId.of("subject-42"), null);
      assertEquals(List.of(SubjectId.of("subject-42")), RecordingPurger.PURGED);
    }
  }

  /**
   * The auto-produced forget evicts the query cache once its purgers have run, so a cached answer
   * holding the subject's data is not served after the erasure.
   */
  @Test
  void forgetEvictsTheProducedQueryCache() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                GdprDefaults.class,
                QueryCacheDefaults.class,
                ForgetSubjectService.class,
                CryptoEngine.class,
                CachingQueryBus.class))) {
      CachingQueryBus bus = arc.container().instance(CachingQueryBus.class).get();
      Map<String, String> readModel = new ConcurrentHashMap<>(Map.of("subject-42", "Alice"));
      bus.register(CustomerRowQuery.class, q -> readModel.getOrDefault(q.customerId(), "none"));
      assertEquals("Alice", bus.dispatch(new CustomerRowQuery("subject-42")));
      readModel.remove("subject-42"); // what a purger does

      arc.container()
          .instance(ForgetSubjectService.class)
          .get()
          .forget(SubjectId.of("subject-42"), null);

      assertEquals(
          "none",
          bus.dispatch(new CustomerRowQuery("subject-42")),
          "the forget must evict the cached answer");
    }
  }
}
