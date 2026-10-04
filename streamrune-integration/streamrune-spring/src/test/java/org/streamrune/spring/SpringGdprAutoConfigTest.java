package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.Cacheable;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.Query;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.gdpr.SubjectDataCollector;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.runtime.CachingQueryBus;
import org.streamrune.runtime.gdpr.ExportSubjectDataService;
import org.streamrune.runtime.gdpr.ForgetSubjectService;
import org.streamrune.test.InMemoryCryptoEngine;

class SpringGdprAutoConfigTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withUserConfiguration(StubEventStoreConfig.class);

  @Test
  void forgetService_present_withCryptoEngineBean() {
    runner
        .withUserConfiguration(CryptoConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(ForgetSubjectService.class);
            });
  }

  @Test
  void forgetService_absent_withoutCryptoEngineBean() {
    runner.run(
        ctx -> {
          assertThat(ctx).doesNotHaveBean(ForgetSubjectService.class);
        });
  }

  @Test
  void exportService_present_withCollectorBean() {
    runner
        .withUserConfiguration(CollectorConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(ExportSubjectDataService.class);
            });
  }

  @Test
  void exportService_absent_withoutCollectorBeans() {
    runner.run(
        ctx -> {
          assertThat(ctx).doesNotHaveBean(ExportSubjectDataService.class);
        });
  }

  /**
   * The auto-configured forget must invoke a registered {@link SubjectDataPurger} — otherwise it
   * crypto-shreds the key but leaves derived PII in read-model projections. Boots the real
   * auto-config graph (does not inject the purger into ForgetSubjectService directly), which is how
   * the wiring gap slipped past the collaborator-injecting unit tests.
   */
  @Test
  void forgetService_invokesRegisteredPurger() {
    runner
        .withUserConfiguration(CryptoConfig.class, PurgerConfig.class)
        .run(
            ctx -> {
              ctx.getBean(ForgetSubjectService.class).forget(SubjectId.of("subject-42"), null);
              RecordingPurger purger = ctx.getBean(RecordingPurger.class);
              assertThat(purger.purged).containsExactly(SubjectId.of("subject-42"));
            });
  }

  /**
   * The auto-configured forget evicts the auto-configured query cache once its purgers have run, so
   * a cached answer holding the subject's data is not served after the erasure.
   */
  @Test
  void forgetService_evictsTheAutoConfiguredQueryCache() {
    runner
        .withPropertyValues("streamrune.query-cache.enabled=true")
        .withUserConfiguration(CryptoConfig.class)
        .run(
            ctx -> {
              CachingQueryBus bus = ctx.getBean(CachingQueryBus.class);
              Map<String, String> readModel =
                  new ConcurrentHashMap<>(Map.of("subject-42", "Alice"));
              bus.register(
                  CustomerRowQuery.class, q -> readModel.getOrDefault(q.customerId(), "none"));
              assertThat(bus.dispatch(new CustomerRowQuery("subject-42"))).isEqualTo("Alice");
              readModel.remove("subject-42"); // what a purger does

              ctx.getBean(ForgetSubjectService.class).forget(SubjectId.of("subject-42"), null);

              assertThat(bus.dispatch(new CustomerRowQuery("subject-42")))
                  .as("the forget must evict the cached answer")
                  .isEqualTo("none");
            });
  }

  @Cacheable(scope = Cacheable.Scope.GLOBAL, ttlSeconds = 3600)
  record CustomerRowQuery(String customerId) implements Query<String> {}

  /**
   * The auto-configured forget service also audits a reinstate, so the GDPR audit trail shows the
   * decision that reverses an erasure next to the erasure itself.
   */
  @Test
  void forgetService_auditsAReinstateBesideTheForgetItReverses() {
    runner
        .withUserConfiguration(CryptoConfig.class, AuditConfig.class)
        .run(
            ctx -> {
              var service = ctx.getBean(ForgetSubjectService.class);
              service.forget(SubjectId.of("subject-42"), UserId.of("dpo"));
              service.reinstate(SubjectId.of("subject-42"), UserId.of("dpo"));
              assertThat(ctx.getBean(RecordingAuditStore.class).saved)
                  .extracting(AuditEntry::commandType)
                  .filteredOn(type -> type.startsWith("GDPR_"))
                  .containsExactly("GDPR_FORGET", "GDPR_REINSTATE");
            });
  }

  /**
   * Two collector beans under one section name: one section would silently replace the other in
   * every export. The auto-configured service refuses to build, so the application fails to start
   * instead of answering access requests with incomplete exports.
   */
  @Test
  void exportService_twoCollectorsUnderOneName_failsStartup() {
    runner
        .withUserConfiguration(CollectorConfig.class, SecondProfileCollectorConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .rootCause()
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("'profile'");
            });
  }

  /**
   * Two purger beans under one name: their outcomes and audit rows could not be told apart. The
   * auto-configured service refuses to build, at startup, before any erasure can run.
   */
  @Test
  void forgetService_twoPurgersUnderOneName_failsStartup() {
    runner
        .withUserConfiguration(CryptoConfig.class, PurgerConfig.class, SecondPurgerConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .rootCause()
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("'recording'");
            });
  }

  @Test
  void forgetService_userOverrideSuppressesAutoConfig() {
    runner
        .withUserConfiguration(CryptoConfig.class, UserForgetServiceConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(ForgetSubjectService.class);
              assertThat(ctx.getBean(ForgetSubjectService.class))
                  .isSameAs(ctx.getBean("userForgetService"));
            });
  }

  @Test
  void exportService_userOverrideSuppressesAutoConfig() {
    runner
        .withUserConfiguration(CollectorConfig.class, UserExportServiceConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(ExportSubjectDataService.class);
              assertThat(ctx.getBean(ExportSubjectDataService.class))
                  .isSameAs(ctx.getBean("userExportService"));
            });
  }

  // ── auto-configuration ORDERING vs the crypto engine configurations ───────────────

  /**
   * Absent explicit ordering, auto-configurations are processed alphabetically — {@code AwsKms… <
   * CryptoProperties… < FileSystem… < Postgres… < StreamRuneAutoConfiguration <
   * VaultCryptoEngineConfiguration}. {@code forgetSubjectService}'s
   * {@code @ConditionalOnBean(CryptoEngine.class)} was therefore evaluated BEFORE the Vault engine
   * bean definition was registered, so the condition failed and a correctly configured Vault
   * deployment silently had NO Article-17 erasure service (or failed startup with a confusing
   * NoSuchBeanDefinitionException when the app injected it). The other three backends happen to
   * sort before "StreamRune" and worked — a sharp, backend-specific asymmetry.
   */
  @Test
  void forgetService_present_withVaultCryptoEngineAutoConfiguration() {
    new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                StreamRuneAutoConfiguration.class,
                CryptoPropertiesConfiguration.class,
                VaultCryptoEngineConfiguration.class))
        .withUserConfiguration(StubEventStoreConfig.class, StubDataSourceConfig.class)
        .withPropertyValues(
            "streamrune.crypto.vault.enabled=true",
            "streamrune.crypto.vault.token=s.test",
            "streamrune.crypto.vault.address=http://localhost:8200")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(CryptoEngine.class);
              assertThat(ctx)
                  .as("a Vault-backed app must get the auto-configured GDPR forget service")
                  .hasSingleBean(ForgetSubjectService.class);
            });
  }

  /**
   * Guards the ordering declaration itself: every {@code *CryptoEngineConfiguration} registered in
   * the auto-configuration imports must be named in {@link StreamRuneAutoConfiguration}'s
   * {@code @AutoConfiguration(after = …)}, so adding a fifth crypto backend cannot silently
   * reintroduce the ordering defect.
   */
  @Test
  void everyCryptoEngineAutoConfigurationIsOrderedBeforeStreamRuneAutoConfiguration()
      throws Exception {
    var resource =
        new org.springframework.core.io.ClassPathResource(
            "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports");
    List<String> engineConfigs;
    try (var in = resource.getInputStream()) {
      engineConfigs =
          new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8)
              .lines()
              .map(String::trim)
              .filter(l -> l.endsWith("CryptoEngineConfiguration"))
              .toList();
    }
    assertThat(engineConfigs).hasSize(4);

    var ordering =
        StreamRuneAutoConfiguration.class.getAnnotation(
            org.springframework.boot.autoconfigure.AutoConfiguration.class);
    assertThat(ordering).isNotNull();
    List<String> after = java.util.Arrays.stream(ordering.after()).map(Class::getName).toList();
    assertThat(after)
        .as(
            "StreamRuneAutoConfiguration must be ordered AFTER every crypto engine configuration —"
                + " its @ConditionalOnBean(CryptoEngine.class) beans cannot see an engine whose"
                + " definition is registered later")
        .containsAll(engineConfigs);
  }

  @Configuration
  static class StubEventStoreConfig {
    @Bean
    EventStoreFactory eventStoreFactory() {
      return () -> mock(EventStore.class);
    }
  }

  @Configuration
  static class StubDataSourceConfig {
    @Bean
    javax.sql.DataSource dataSource() {
      return mock(javax.sql.DataSource.class);
    }
  }

  @Configuration
  static class CryptoConfig {
    @Bean
    CryptoEngine cryptoEngine() {
      return new InMemoryCryptoEngine();
    }
  }

  @Configuration
  static class CollectorConfig {
    @Bean
    SubjectDataCollector profileCollector() {
      return new SubjectDataCollector() {
        @Override
        public String name() {
          return "profile";
        }

        @Override
        public JsonNode collect(SubjectId subjectId) {
          return new ObjectMapper().createObjectNode();
        }
      };
    }
  }

  @Configuration
  static class SecondProfileCollectorConfig {
    @Bean
    SubjectDataCollector legacyProfileCollector() {
      return new SubjectDataCollector() {
        @Override
        public String name() {
          return "profile";
        }

        @Override
        public JsonNode collect(SubjectId subjectId) {
          return new ObjectMapper().createObjectNode();
        }
      };
    }
  }

  @Configuration
  static class SecondPurgerConfig {
    @Bean
    RecordingPurger secondRecordingPurger() {
      return new RecordingPurger();
    }
  }

  @Configuration
  static class AuditConfig {
    @Bean
    RecordingAuditStore recordingAuditStore() {
      return new RecordingAuditStore();
    }
  }

  static class RecordingAuditStore implements AuditStore {
    final List<AuditEntry> saved = new CopyOnWriteArrayList<>();

    @Override
    public void save(AuditEntry entry) {
      saved.add(entry);
    }
  }

  @Configuration
  static class PurgerConfig {
    @Bean
    RecordingPurger recordingPurger() {
      return new RecordingPurger();
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

  @Configuration
  static class UserForgetServiceConfig {
    @Bean
    ForgetSubjectService userForgetService() {
      return ForgetSubjectService.builder().cryptoEngine(new InMemoryCryptoEngine()).build();
    }
  }

  @Configuration
  static class UserExportServiceConfig {
    @Bean
    ExportSubjectDataService userExportService() {
      return ExportSubjectDataService.builder().collectors(java.util.List.of()).build();
    }
  }
}
