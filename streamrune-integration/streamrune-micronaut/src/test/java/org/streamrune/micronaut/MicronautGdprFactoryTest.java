package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.context.ApplicationContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Cacheable;
import org.streamrune.core.Query;
import org.streamrune.core.gdpr.SnapshotPurger;
import org.streamrune.core.gdpr.SubjectDataCollector;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.SubjectId;
import org.streamrune.runtime.CachingQueryBus;
import org.streamrune.runtime.gdpr.ExportSubjectDataService;
import org.streamrune.runtime.gdpr.ForgetSubjectService;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * Tests that {@link StreamRuneMicronautModule}'s GDPR factory methods are gated by their
 * {@code @Requires(beans = ...)} guards.
 *
 * <p>Each test starts an {@link ApplicationContext} with (or without) the required singleton
 * pre-registered via {@code ApplicationContext.builder().singletons(...).start()}, which ensures
 * the condition is evaluated during bean discovery — before the factory method is called.
 */
class MicronautGdprFactoryTest {

  /**
   * The tests of everything but the snapshot purge state that their application stores no
   * snapshots; the snapshot purge is tested below without it.
   */
  private static final Map<String, Object> NO_SNAPSHOTS =
      Map.of("streamrune.gdpr.snapshot-purge", "none");

  @Test
  void forgetService_present_withCryptoEngineBean() {
    try (var ctx =
        ApplicationContext.builder()
            .properties(NO_SNAPSHOTS)
            .singletons(new InMemoryCryptoEngine())
            .start()) {
      assertTrue(ctx.containsBean(ForgetSubjectService.class));
    }
  }

  /**
   * The auto-configured forget needs a snapshot purger: a snapshot keeps every field of the
   * aggregate's state not marked {@code @Encrypted} in the clear, so a forget without one leaves
   * the subject's data readable. The context does not start — the startup validator builds the
   * service — and the message says what to declare and which property opts out.
   */
  @Test
  void forgetService_withoutASnapshotPurgerBean_failsStartupSayingWhatToAdd() {
    var failure =
        assertThrows(
            RuntimeException.class,
            () ->
                ApplicationContext.builder()
                    .singletons(new InMemoryCryptoEngine(), new RecordingPurger())
                    .start()
                    .close());
    assertTrue(
        causeChainContains(
            failure,
            "The ForgetSubjectService StreamRune configures has no snapshot purger.",
            "new PostgresSnapshotStorePurger(dataSource,",
            "or set streamrune.gdpr.snapshot-purge=none"),
        () -> "the startup failure says what to add: " + failure);
  }

  /** A {@link SnapshotPurger} bean is run by the auto-configured forget like any other purger. */
  @Test
  void forgetService_runsASnapshotPurgerBeanBesideTheReadModelPurgers() {
    var purger = new RecordingPurger();
    var snapshots = new RecordingSnapshotPurger();
    try (var ctx =
        ApplicationContext.builder()
            .singletons(new InMemoryCryptoEngine(), purger, snapshots)
            .start()) {
      var result = ctx.getBean(ForgetSubjectService.class).forget(SubjectId.of("subject-42"), null);
      assertEquals(List.of(SubjectId.of("subject-42")), snapshots.purged);
      assertEquals(List.of(SubjectId.of("subject-42")), purger.purged);
      assertTrue(result.fullyErased());
    }
  }

  /** {@code none} beside a snapshot purger bean contradicts itself: startup fails. */
  @Test
  void forgetService_snapshotPurgeNoneWithASnapshotPurgerBean_failsStartup() {
    var failure =
        assertThrows(
            RuntimeException.class,
            () ->
                ApplicationContext.builder()
                    .properties(NO_SNAPSHOTS)
                    .singletons(new InMemoryCryptoEngine(), new RecordingSnapshotPurger())
                    .start()
                    .close());
    assertTrue(
        causeChainContains(
            failure,
            "streamrune.gdpr.snapshot-purge=none says this application has never stored a"
                + " snapshot, but a snapshot purger bean is registered ("
                + RecordingSnapshotPurger.class.getName()
                + ")"),
        () -> "the startup failure names the contradiction: " + failure);
  }

  /** An unknown value fails startup, with or without a crypto engine. */
  @Test
  void snapshotPurge_anUnknownValueFailsStartupNamingTheProperty() {
    var failure =
        assertThrows(
            RuntimeException.class,
            () ->
                ApplicationContext.builder()
                    .properties(Map.of("streamrune.gdpr.snapshot-purge", "maybe"))
                    .start()
                    .close());
    assertTrue(
        causeChainContains(
            failure,
            "streamrune.gdpr.snapshot-purge must be required or none (case-insensitive); got:"
                + " 'maybe'"),
        () -> "the startup failure names the property: " + failure);
  }

  /** Without a crypto engine there is no forget service, and no snapshot purger is asked for. */
  @Test
  void noCryptoEngine_needsNoSnapshotPurger() {
    try (var ctx = ApplicationContext.builder().start()) {
      assertFalse(ctx.containsBean(ForgetSubjectService.class));
    }
  }

  private static boolean causeChainContains(Throwable failure, String... fragments) {
    for (Throwable t = failure; t != null; t = t.getCause()) {
      String message = t.getMessage();
      if (message != null && java.util.Arrays.stream(fragments).allMatch(message::contains)) {
        return true;
      }
    }
    return false;
  }

  @Test
  void forgetService_absent_withoutCryptoEngineBean() {
    try (var ctx = ApplicationContext.builder().start()) {
      assertFalse(ctx.containsBean(ForgetSubjectService.class));
    }
  }

  @Test
  void exportService_present_withCollectorBean() {
    try (var ctx = ApplicationContext.builder().singletons(new ProfileCollector()).start()) {
      assertTrue(ctx.containsBean(ExportSubjectDataService.class));
    }
  }

  @Test
  void exportService_absent_withoutCollectorBeans() {
    try (var ctx = ApplicationContext.builder().start()) {
      assertFalse(ctx.containsBean(ExportSubjectDataService.class));
    }
  }

  /**
   * The auto-configured forget must invoke a registered {@link SubjectDataPurger}. Boots the real
   * Micronaut context (does not call the factory method with a hand-passed purger), which is how
   * the missing {@code List<SubjectDataPurger>} injection slipped past the direct-call unit tests.
   */
  @Test
  void forgetService_invokesRegisteredPurger() {
    var purger = new RecordingPurger();
    try (var ctx =
        ApplicationContext.builder()
            .properties(NO_SNAPSHOTS)
            .singletons(new InMemoryCryptoEngine(), purger)
            .start()) {
      ctx.getBean(ForgetSubjectService.class).forget(SubjectId.of("subject-42"), null);
      assertEquals(List.of(SubjectId.of("subject-42")), purger.purged);
    }
  }

  /**
   * The auto-configured forget evicts the auto-configured query cache once its purgers have run, so
   * a cached answer holding the subject's data is not served after the erasure.
   */
  @Test
  void forgetService_evictsTheAutoConfiguredQueryCache() {
    try (var ctx =
        ApplicationContext.builder()
            .properties(
                Map.of(
                    "streamrune.query-cache.enabled",
                    "true",
                    "streamrune.gdpr.snapshot-purge",
                    "none"))
            .singletons(new InMemoryCryptoEngine())
            .start()) {
      CachingQueryBus bus = ctx.getBean(CachingQueryBus.class);
      Map<String, String> readModel = new ConcurrentHashMap<>(Map.of("subject-42", "Alice"));
      bus.register(CustomerRowQuery.class, q -> readModel.getOrDefault(q.customerId(), "none"));
      assertEquals("Alice", bus.dispatch(new CustomerRowQuery("subject-42")));
      readModel.remove("subject-42"); // what a purger does

      ctx.getBean(ForgetSubjectService.class).forget(SubjectId.of("subject-42"), null);

      assertEquals(
          "none",
          bus.dispatch(new CustomerRowQuery("subject-42")),
          "the forget must evict the cached answer");
    }
  }

  @Cacheable(scope = Cacheable.Scope.GLOBAL, ttlSeconds = 3600)
  record CustomerRowQuery(String customerId) implements Query<String> {}

  static class ProfileCollector implements SubjectDataCollector {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public String name() {
      return "profile";
    }

    @Override
    public JsonNode collect(SubjectId subjectId) {
      return MAPPER.createObjectNode();
    }
  }

  static class RecordingSnapshotPurger implements SnapshotPurger {
    final List<SubjectId> purged = new CopyOnWriteArrayList<>();

    @Override
    public String name() {
      return "snapshots";
    }

    @Override
    public void purge(SubjectId subjectId) {
      purged.add(subjectId);
    }
  }

  static class RecordingPurger implements SubjectDataPurger {
    final List<SubjectId> purged = new CopyOnWriteArrayList<>();

    @Override
    public String name() {
      return "recording";
    }

    @Override
    public void purge(SubjectId subjectId) {
      purged.add(subjectId);
    }
  }
}
