package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

  @Test
  void forgetService_present_withCryptoEngineBean() {
    try (var ctx = ApplicationContext.builder().singletons(new InMemoryCryptoEngine()).start()) {
      assertTrue(ctx.containsBean(ForgetSubjectService.class));
    }
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
        ApplicationContext.builder().singletons(new InMemoryCryptoEngine(), purger).start()) {
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
            .properties(Map.of("streamrune.query-cache.enabled", "true"))
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
