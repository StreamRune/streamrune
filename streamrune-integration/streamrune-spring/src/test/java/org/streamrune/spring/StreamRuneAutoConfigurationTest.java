package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.Command;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.types.AggregateId;
import org.streamrune.runtime.AnnotationAuthorizationInterceptor;
import org.streamrune.runtime.CircuitBreakerCommandInterceptor;
import org.streamrune.runtime.VirtualThreadCommandBus;

/** Tests for {@link StreamRuneAutoConfiguration}. */
class StreamRuneAutoConfigurationTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withUserConfiguration(StubDataSourceConfig.class, StubEventStoreConfig.class);

  // The servlet ScopedValueFilter beans are @ConditionalOnWebApplication(SERVLET), so a
  // servlet web context is required to assert their presence.
  private final WebApplicationContextRunner webContextRunner =
      new WebApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withUserConfiguration(StubDataSourceConfig.class, StubEventStoreConfig.class);

  @Test
  void headlessNonWebApp_startsWithoutServletFilterBeans() {
    // A headless worker (WebApplicationType.NONE + starter + DataSource, no web starter) has
    // no jakarta.servlet.Filter on the runtime classpath. The unconditional scopedValueFilter /
    // scopedValueFilterRegistration beans crashed context refresh with NoClassDefFoundError:
    // jakarta/servlet/Filter. Gating them on @ConditionalOnWebApplication(SERVLET) means a non-web
    // context (here also with servlet.Filter filtered off the classpath) starts cleanly with
    // neither bean present.
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withUserConfiguration(StubDataSourceConfig.class, StubEventStoreConfig.class)
        .withClassLoader(new FilteredClassLoader(jakarta.servlet.Filter.class))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).doesNotHaveBean("scopedValueFilter");
              assertThat(ctx).doesNotHaveBean("scopedValueFilterRegistration");
              // the rest of the framework still wires up in a headless worker
              assertThat(ctx).hasSingleBean(VirtualThreadCommandBus.class);
              assertThat(ctx).hasSingleBean(EventStore.class);
            });
  }

  @Test
  void registersDefaultBeans() {
    // ScopedValueFilter is servlet-web-only, so assert it under a servlet web context.
    webContextRunner.run(ctx -> assertThat(ctx).hasSingleBean(ScopedValueFilter.class));
    contextRunner.run(
        ctx -> {
          assertThat(ctx).hasSingleBean(AggregateLocker.class);
          assertThat(ctx).hasSingleBean(VirtualThreadCommandBus.class);
          assertThat(ctx).hasSingleBean(StreamRuneProperties.class);
          assertThat(ctx).hasSingleBean(EventStore.class);
          assertThat(ctx).hasSingleBean(EventStoreFactory.class);
          assertThat(ctx).hasSingleBean(EventTypeRegistry.class);
          assertThat(ctx.getBean(EventTypeRegistry.class))
              .isInstanceOf(SimpleEventTypeRegistry.class);
        });
  }

  @Test
  void subscriptionHealthContributorExistsInTheDefaultConfiguration() {
    // The contributor was gated on @ConditionalOnBean({EventStore, OffsetStore}), but
    // the auto-configured postgresOffsetStore is declared ~750 lines LATER in the same
    // configuration class. Spring registers a configuration class's @Bean definitions in
    // declaration order and evaluates REGISTER_BEAN-phase conditions against the registry as it
    // goes, so no OffsetStore definition existed yet and the condition always failed: every default
    // Spring deployment silently had NO subscription health/lag tracking — /health kept reporting
    // UP
    // with a terminally halted continuous projection (frozen read model) and the documented
    // streamrune.subscriptions.lag gauge was never emitted. It only worked when the application
    // hand-defined an OffsetStore bean (user configs are registered before auto-configurations).
    contextRunner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          assertThat(ctx).hasSingleBean(org.streamrune.postgres.PostgresOffsetStore.class);
          assertThat(ctx).hasSingleBean(org.streamrune.runtime.SubscriptionHealthContributor.class);
          assertThat(
                  ctx.getBeanProvider(org.streamrune.runtime.SubscriptionHealthContributor.class)
                      .getIfAvailable())
              .as("the contributor must be a real instance, not a skipped/null bean")
              .isNotNull();
        });
  }

  @Test
  void subscriptionHealthLagThresholdIsBoundFromItsProperty() {
    contextRunner
        .withPropertyValues("streamrune.subscription.health.lag-threshold=25000")
        .run(
            ctx ->
                assertThat(
                        ctx.getBean(org.streamrune.runtime.SubscriptionHealthContributor.class)
                            .lagThreshold())
                    .isEqualTo(25_000L));
  }

  @Test
  void subscriptionHealthLagThresholdDefaultsToOneThousandEvents() {
    contextRunner.run(
        ctx ->
            assertThat(
                    ctx.getBean(org.streamrune.runtime.SubscriptionHealthContributor.class)
                        .lagThreshold())
                .isEqualTo(1_000L));
  }

  @Test
  void aSubscriptionHealthLagThresholdBelowOneFailsTheStartNamingTheProperty() {
    contextRunner
        .withPropertyValues("streamrune.subscription.health.lag-threshold=0")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .rootCause()
                  .hasMessageContaining(
                      "streamrune.subscription.health.lag-threshold must be at least 1");
            });
  }

  @Test
  void subscriptionHealthContributorAbsentWithoutAnOffsetStore() {
    // No DataSource → no auto-configured OffsetStore → the contributor must degrade to absent
    // (Spring skips a null @Bean product) rather than fail context refresh.
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withUserConfiguration(StubEventStoreConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).doesNotHaveBean(org.streamrune.core.projection.OffsetStore.class);
              assertThat(
                      ctx.getBeanProvider(
                              org.streamrune.runtime.SubscriptionHealthContributor.class)
                          .getIfAvailable())
                  .isNull();
            });
  }

  @Test
  void userDefinedLockerOverridesDefault() {
    contextRunner
        .withUserConfiguration(CustomLockerConfig.class)
        .run(
            ctx -> {
              AggregateLocker locker = ctx.getBean(AggregateLocker.class);
              assertThat(locker).isInstanceOf(CustomLocker.class);
            });
  }

  @Test
  void shouldRegisterQueryBusBean() {
    contextRunner.run(
        ctx -> {
          assertThat(ctx).hasBean("queryBus");
          // Primary QueryBus wraps AuditStore when DataSource is available
          assertThat(ctx.getBean("queryBus", org.streamrune.core.QueryBus.class))
              .isInstanceOf(org.streamrune.runtime.AuditingQueryBus.class);
        });
  }

  /**
   * A user-defined QueryBus under a name other than "queryBus" must suppress the
   * framework's @Primary composite. Before the fix the framework backed off only on the bean NAME
   * "queryBus", so a differently-named user bus left the framework composing its own @Primary bus
   * that silently won every injection point — the user's decorator was instantiated but never used.
   * Now the framework backs off on TYPE (ignoring only its own SimpleQueryBus/CachingQueryBus
   * building blocks).
   */
  @Test
  void userDefinedQueryBusUnderAnotherName_winsOverFrameworkPrimary() {
    contextRunner
        .withUserConfiguration(UserQueryBusConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(org.streamrune.core.QueryBus.class))
                  .as("the user's QueryBus must win, not the framework @Primary composite")
                  .isSameAs(UserQueryBusConfig.USER_BUS);
            });
  }

  @Configuration
  static class UserQueryBusConfig {
    static final org.streamrune.core.QueryBus USER_BUS = mock(org.streamrune.core.QueryBus.class);

    @Bean
    @Primary
    org.streamrune.core.QueryBus tenantFilteringQueryBus() {
      return USER_BUS;
    }
  }

  @Test
  void sseControllerDisabledByDefault() {
    // The SSE endpoint must be off by default (it streams decrypted events by stream
    // id).
    contextRunner.run(
        ctx -> {
          assertThat(ctx).hasSingleBean(org.streamrune.runtime.SseEventPublisher.class);
          assertThat(ctx).doesNotHaveBean(SseController.class);
          assertThat(ctx).doesNotHaveBean(org.streamrune.integration.SseAuthorizer.class);
          assertThat(ctx).doesNotHaveBean(org.streamrune.runtime.SseEventFeed.class);
        });
  }

  /**
   * The endpoint and its feed come and go together: the feed that publishes the stored events is
   * registered, and running, exactly where the controller is.
   */
  @Test
  void sseEventFeedRunsWhereTheEndpointIsEnabled() {
    webContextRunner
        .withPropertyValues("streamrune.sse.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(org.streamrune.runtime.SseEventFeed.class);
              assertThat(ctx.getBean(org.streamrune.runtime.SseEventFeed.class).isRunning())
                  .isTrue();
            });
  }

  @Test
  void sseControllerRegisteredWhenEnabledWithFailClosedAuthorizer() {
    // The endpoint is Spring-MVC-bound (stream() returns SseEmitter), so it now requires a
    // servlet web application — assert it under the servlet web context.
    webContextRunner
        .withPropertyValues("streamrune.sse.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(SseController.class);
              // Fail-closed: a deny-all authorizer is installed when the app provides none.
              assertThat(ctx.getBean(org.streamrune.integration.SseAuthorizer.class))
                  .isSameAs(org.streamrune.integration.SseAuthorizer.DENY_ALL);
            });
  }

  /**
   * Enabling SSE on a NON-servlet context (a headless worker sharing the web app's {@code
   * application.yml}, or a WebFlux/Jersey stack) must leave the endpoint unregistered rather than
   * create an MVC-bound controller the stack cannot serve — and must not fail boot. The publisher
   * itself is stack-independent and stays available to application code.
   */
  @Test
  void sseControllerNotRegisteredOnANonServletContext() {
    contextRunner
        .withPropertyValues("streamrune.sse.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).doesNotHaveBean(SseController.class);
              assertThat(ctx).doesNotHaveBean(org.streamrune.integration.SseAuthorizer.class);
              assertThat(ctx).hasSingleBean(org.streamrune.runtime.SseEventPublisher.class);
              // No endpoint, so nothing reads the global stream for it.
              assertThat(ctx).doesNotHaveBean(org.streamrune.runtime.SseEventFeed.class);
            });
  }

  @Test
  void userDefinedSseEventPublisherOverridesDefault() {
    var custom = new org.streamrune.runtime.SseEventPublisher();
    webContextRunner
        .withPropertyValues("streamrune.sse.enabled=true")
        .withBean(org.streamrune.runtime.SseEventPublisher.class, () -> custom)
        .run(
            ctx -> {
              assertThat(ctx.getBean(org.streamrune.runtime.SseEventPublisher.class))
                  .isSameAs(custom);
              assertThat(ctx).hasSingleBean(SseController.class);
            });
  }

  /**
   * {@code streamrune.sse.timeout=0} means "no deadline" on Quarkus and Micronaut (both ports'
   * {@code positiveOrNull} helper), and both frameworks' property docs advertise parity with
   * Spring. Spring silently mapped the same value back to the 5-minute default, so an operator who
   * disabled the cap to keep a live dashboard open had every client dropped every five minutes with
   * no way to turn the cap off at all. One contract now: non-positive disables the deadline —
   * {@code SseEmitter(0)} is the servlet-async "never time out" value — while the keepalive stays
   * the primary dead-client reaper.
   */
  @Test
  void sseTimeoutZeroDisablesTheDeadline() {
    webContextRunner
        .withPropertyValues("streamrune.sse.enabled=true", "streamrune.sse.timeout=0")
        .withBean(
            org.streamrune.integration.SseAuthorizer.class, () -> (principal, streamId) -> true)
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(SseController.class);
              var emitter = ctx.getBean(SseController.class).stream("cart", "cart-1", List.of());
              assertThat(emitter.getTimeout())
                  .as(
                      "non-positive streamrune.sse.timeout must disable the deadline, not fall"
                          + " back to the 5-minute default")
                  .isZero();
            });
  }

  @Test
  void sseTimeoutUnsetKeepsTheFiniteDefault() {
    // The default stays finite: only an explicit non-positive value opts out of the
    // backstop.
    webContextRunner
        .withPropertyValues("streamrune.sse.enabled=true")
        .withBean(
            org.streamrune.integration.SseAuthorizer.class, () -> (principal, streamId) -> true)
        .run(
            ctx ->
                assertThat(
                        ctx.getBean(SseController.class).stream("cart", "cart-2", List.of())
                            .getTimeout())
                    .isEqualTo(java.time.Duration.ofMinutes(5).toMillis()));
  }

  /**
   * Package prefixes a deployment can legitimately be missing: the {@code compileOnly}
   * (classpath-optional) dependencies of {@code streamrune-spring} — the servlet API, Bean
   * Validation, Spring Boot's health/actuator modules, Spring MVC/WebFlux, OpenTelemetry and Spring
   * Security — plus the AWS SDK.
   *
   * <p>The AWS SDK ships as a {@code runtime} dependency rather than {@code compileOnly}, but it is
   * ~10 MB that only KMS-backed crypto needs, so excluding it is a routine and supported thing for
   * an application to do — and {@code AwsKmsCryptoEngineConfiguration} is registered in {@code
   * AutoConfiguration.imports} unconditionally, so Spring introspects it on every boot regardless
   * of {@code streamrune.crypto.aws.enabled}. A top-level signature naming an AWS type there is
   * therefore exactly the optional-dependency failure mode: context refresh dies before any
   * condition runs. The client already sits behind a StreamRune-owned holder; this keeps it there.
   */
  private static final java.util.List<String> OPTIONAL_DEPENDENCY_PACKAGES =
      java.util.List.of(
          "jakarta.servlet.",
          "jakarta.validation.",
          "org.springframework.boot.health.",
          "org.springframework.boot.actuate.",
          "org.springframework.web.servlet.",
          "org.springframework.web.reactive.",
          "io.opentelemetry.",
          "org.springframework.security.",
          "software.amazon.awssdk.");

  /** The registration file Spring reads to discover this module's auto-configurations. */
  private static final String AUTO_CONFIGURATION_IMPORTS =
      "META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports";

  /**
   * Generalized guard: <strong>no</strong> method declared directly on <em>any</em> registered
   * top-level auto-configuration class may name a type from a classpath-optional dependency —
   * neither directly, nor through the supertype closure of a framework type it names.
   *
   * <p>{@code Class#getDeclaredMethods} resolves every declared method's return and parameter types
   * (synthetic lambda methods included), and defining a class resolves its superclass and
   * superinterfaces. One such reference makes the whole configuration class fail to introspect on a
   * classpath where the jar is genuinely absent — before any {@code @ConditionalOnClass} /
   * {@code @ConditionalOnWebApplication} gate is consulted, since those only suppress bean
   * REGISTRATION. That is exactly how {@code scopedValueFilter} (→ {@code jakarta.servlet.Filter}),
   * {@code streamRuneHealthIndicator} (→ Boot's {@code HealthIndicator}) and the {@code
   * beanValidationInterceptor} fallback lambda (whose synthetic method RETURNS {@code
   * jakarta.validation.Validator}) broke WebFlux and headless deployments.
   *
   * <p>Nested {@code @Configuration} classes are exempt by design — that is the remedy (the nesting
   * pattern): the gate lives on the nested class, so the enclosing class is introspected without
   * ever naming the optional type. This test is the cheap always-on guard; {@code
   * OptionalDependencyAbsentTest} proves the real boot on a genuinely stripped classpath.
   *
   * <p><b>Covers every registered auto-configuration, read from {@code AutoConfiguration.imports}
   * at test time.</b> The guard previously walked only {@code StreamRuneAutoConfiguration}, while
   * that file registers seven top-level classes — so a signature defect in any of the other six was
   * invisible to it. That is not hypothetical: {@code AwsKmsCryptoEngineConfiguration}'s {@code
   * KmsClient kmsClient(...)} used to be declared directly on it, and that pre-fix signature is
   * exactly what this guard exists to catch — it named an AWS type on a class Spring introspects on
   * every boot. Reading the registration file (rather than hardcoding the list) means a future
   * auto-configuration is covered the moment it is registered.
   */
  @Test
  void topLevelAutoConfigurationSignaturesNeverNameAnOptionalDependencyType() throws Exception {
    // Positive control: the closure walker must genuinely see THROUGH a framework type to the
    // optional supertype that breaks linking, or the assertion below would pass vacuously.
    assertThat(supertypeClosure(ScopedValueFilter.class)).contains(jakarta.servlet.Filter.class);
    assertThat(supertypeClosure(StreamRuneHealthIndicator.class))
        .contains(org.springframework.boot.health.contributor.HealthIndicator.class);
    // Positive control: the detector fires on a signature shaped like the defects it guards against
    // — a @Bean method returning an optional-dependency type, which is precisely the shape
    // AwsKmsCryptoEngineConfiguration carried before its client moved behind a holder.
    assertThat(optionalTypeViolationsIn(PreFixSignatureShapes.class))
        .as("the detector itself must fire, or every assertion below is vacuous")
        .anyMatch(
            v ->
                v.contains("#kmsClient")
                    && v.contains("software.amazon.awssdk.services.kms.KmsClient"))
        .anyMatch(v -> v.contains("#scopedValueFilter") && v.contains("jakarta.servlet.Filter"));

    var autoConfigurations = registeredAutoConfigurations();
    // Coverage control: the guard must span the WHOLE registration file, crypto configurations
    // included — walking only StreamRuneAutoConfiguration is the gap this test closes.
    assertThat(autoConfigurations)
        .as("read from " + AUTO_CONFIGURATION_IMPORTS)
        .hasSizeGreaterThanOrEqualTo(7)
        .contains(
            StreamRuneAutoConfiguration.class,
            ProjectionAutoConfig.class,
            CryptoPropertiesConfiguration.class,
            FileSystemCryptoEngineConfiguration.class,
            PostgresCryptoEngineConfiguration.class,
            VaultCryptoEngineConfiguration.class,
            AwsKmsCryptoEngineConfiguration.class);

    var violations = new java.util.ArrayList<String>();
    for (Class<?> autoConfiguration : autoConfigurations) {
      violations.addAll(optionalTypeViolationsIn(autoConfiguration));
    }
    assertThat(violations)
        .as(
            "move these @Bean methods into a nested @Configuration gated by"
                + " @ConditionalOnClass(name=...), or behind a StreamRune-owned holder type — a"
                + " top-level signature naming an optional type breaks context refresh when the jar"
                + " is absent, before any condition runs")
        .isEmpty();
  }

  /**
   * Every class listed in this module's {@code AutoConfiguration.imports}, loaded but NOT
   * initialized. Reading the file is the point: it is the same list Spring reads, so a newly
   * registered auto-configuration is covered without touching this test.
   */
  private static java.util.List<Class<?>> registeredAutoConfigurations() throws Exception {
    var loader = StreamRuneAutoConfigurationTest.class.getClassLoader();
    var resource = loader.getResource(AUTO_CONFIGURATION_IMPORTS);
    assertThat(resource)
        .as(AUTO_CONFIGURATION_IMPORTS + " must be on the test classpath")
        .isNotNull();
    var classes = new java.util.ArrayList<Class<?>>();
    try (var reader =
        new java.io.BufferedReader(
            new java.io.InputStreamReader(
                resource.openStream(), java.nio.charset.StandardCharsets.UTF_8))) {
      for (String line; (line = reader.readLine()) != null; ) {
        String name = line.strip();
        if (name.isEmpty() || name.startsWith("#")) {
          continue;
        }
        classes.add(Class.forName(name, false, loader));
      }
    }
    return classes;
  }

  /**
   * Optional-dependency references in {@code autoConfiguration}'s own declared methods. A failure
   * to even enumerate them is itself reported as a violation: {@code Class#getDeclaredMethods}
   * throwing {@link NoClassDefFoundError} IS the optional-dependency defect (a signature naming an
   * absent type), so it must never surface as a test error that looks like harness breakage.
   */
  private static java.util.List<String> optionalTypeViolationsIn(Class<?> autoConfiguration) {
    java.lang.reflect.Method[] declaredMethods;
    try {
      declaredMethods = autoConfiguration.getDeclaredMethods();
    } catch (Throwable linkageFailure) {
      return java.util.List.of(
          autoConfiguration.getName()
              + " cannot even be introspected on this classpath: "
              + linkageFailure);
    }
    var violations = new java.util.ArrayList<String>();
    for (var method : declaredMethods) {
      var referenced = new java.util.ArrayList<Class<?>>();
      referenced.add(method.getReturnType());
      referenced.addAll(java.util.List.of(method.getParameterTypes()));
      for (Class<?> type : referenced) {
        for (Class<?> linked : supertypeClosure(type)) {
          OPTIONAL_DEPENDENCY_PACKAGES.stream()
              .filter(prefix -> linked.getName().startsWith(prefix))
              .findFirst()
              .ifPresent(
                  prefix ->
                      violations.add(
                          autoConfiguration.getSimpleName()
                              + "#"
                              + method.getName()
                              + " references "
                              + type.getName()
                              + (linked.equals(type)
                                  ? ""
                                  : " (which links " + linked.getName() + ")")));
        }
      }
    }
    return violations;
  }

  /**
   * Positive control for {@link #optionalTypeViolationsIn}: the two signature shapes the guard
   * exists to catch, reproduced verbatim. {@code kmsClient} is {@code
   * AwsKmsCryptoEngineConfiguration}'s pre-holder signature — the case the old guard could not see,
   * because it walked only {@code StreamRuneAutoConfiguration}. {@code scopedValueFilter} is the
   * original case, reached through the supertype closure.
   */
  static final class PreFixSignatureShapes {
    software.amazon.awssdk.services.kms.KmsClient kmsClient(StreamRuneCryptoProperties props) {
      throw new UnsupportedOperationException(String.valueOf(props));
    }

    ScopedValueFilter scopedValueFilter() {
      throw new UnsupportedOperationException();
    }
  }

  /** {@code type} plus every superclass and (transitive) superinterface — what linking resolves. */
  private static java.util.Set<Class<?>> supertypeClosure(Class<?> type) {
    var seen = new java.util.LinkedHashSet<Class<?>>();
    var queue = new java.util.ArrayDeque<Class<?>>();
    queue.add(type);
    while (!queue.isEmpty()) {
      Class<?> current = queue.poll();
      if (current == Object.class || current.isPrimitive() || !seen.add(current)) {
        continue;
      }
      // ArrayDeque rejects nulls; interfaces and primitives have no superclass.
      Class<?> superclass = current.getSuperclass();
      if (superclass != null) {
        queue.add(superclass);
      }
      queue.addAll(java.util.List.of(current.getInterfaces()));
    }
    return seen;
  }

  @Test
  void circuitBreakerInterceptorIsRegistered() {
    contextRunner.run(
        ctx -> {
          assertThat(ctx).hasSingleBean(CircuitBreakerCommandInterceptor.class);
        });
  }

  @Test
  void luminaPropertiesDefaultsAreBound() {
    contextRunner.run(
        ctx -> {
          StreamRuneProperties props = ctx.getBean(StreamRuneProperties.class);
          assertThat(props.snapshotEveryNEvents()).isEqualTo(100);
          assertThat(props.retryMaxAttempts()).isEqualTo(3);
          assertThat(props.stripeCount()).isEqualTo(1024);
        });
  }

  // ── the snapshot schema version must be reachable ────────────────────────────────

  /**
   * The auto-config hardcoded {@code SnapshotPolicy.everyNEvents(n)} — the ONE-ARG overload, which
   * pins {@code snapshotVersion} to 1 — so the documented schema-version bump was unreachable
   * through the auto-configuration and every registered {@code SnapshotMigration} bean was dead
   * code (the store's mismatch branch can only be entered when the expected version differs from
   * the stored one). The consequence on the DEFAULT configuration (snapshots on, every 100 events)
   * is a silent wrong answer: after a state-class change the OLD payload is deserialized into the
   * NEW record, Jackson defaults the missing field, the post-snapshot replay is narrowed away, and
   * the aggregate decides against state it never really had.
   *
   * <p>Observed through the wired bus, not by hand: the version the bus passes to {@code
   * EventStore.load(streamId, expectedSnapshotVersion)} IS the effective snapshot schema version.
   */
  @Test
  void applicationSuppliedSnapshotPolicyReachesTheCommandBus() {
    RecordingEventStoreConfig.LAST_EXPECTED_SNAPSHOT_VERSION.set(-1);
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withUserConfiguration(
            StubDataSourceConfig.class,
            RecordingEventStoreConfig.class,
            SnapshotProbeDeciderConfig.class,
            BumpedSnapshotPolicyConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              ctx.getBean(VirtualThreadCommandBus.class)
                  .execute(new SnapshotProbeCommand("agg-snap-1"));
              assertThat(RecordingEventStoreConfig.LAST_EXPECTED_SNAPSHOT_VERSION.get())
                  .as(
                      "an application SnapshotPolicy bean must drive the bus's expected snapshot"
                          + " schema version — otherwise the bump, and every SnapshotMigration, is"
                          + " unreachable")
                  .isEqualTo(2);
            });
  }

  /** The default side: with no application policy bean the shipped behaviour is unchanged. */
  @Test
  void defaultSnapshotPolicyIsEveryNEventsAtSchemaVersionOne() {
    RecordingEventStoreConfig.LAST_EXPECTED_SNAPSHOT_VERSION.set(-1);
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withUserConfiguration(
            StubDataSourceConfig.class,
            RecordingEventStoreConfig.class,
            SnapshotProbeDeciderConfig.class)
        .run(
            ctx -> {
              ctx.getBean(VirtualThreadCommandBus.class)
                  .execute(new SnapshotProbeCommand("agg-snap-2"));
              assertThat(RecordingEventStoreConfig.LAST_EXPECTED_SNAPSHOT_VERSION.get())
                  .isEqualTo(1);
            });
  }

  /**
   * {@code SnapshotPolicy.never()} — which {@code docs/guide/production.md} tells operators to
   * start with — was equally unreachable: {@code streamrune.snapshot-every-n-events} is validated
   * {@code > 0}, so snapshots could not be switched off either. A policy bean reaches it; the bus
   * then passes {@code EventStore.IGNORE_SNAPSHOT}, the store's "never consult a snapshot at all"
   * sentinel — distinct from {@code 0}, "accept whatever is stored," which would silently rehydrate
   * a snapshot left over from before the policy switch.
   */
  @Test
  void applicationSuppliedNeverPolicyDisablesSnapshotting() {
    // never() must route through EventStore.IGNORE_SNAPSHOT, not the old "skip the
    // version check, use any stored snapshot" sentinel (0) — a snapshot left over from before the
    // policy switched to never() must never be silently rehydrated. The "not yet observed" marker
    // must differ from IGNORE_SNAPSHOT (-1), now a real expected value here, so a wiring failure
    // that never reaches the store cannot coincidentally read as a correct observation.
    RecordingEventStoreConfig.LAST_EXPECTED_SNAPSHOT_VERSION.set(Integer.MIN_VALUE);
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withUserConfiguration(
            StubDataSourceConfig.class,
            RecordingEventStoreConfig.class,
            SnapshotProbeDeciderConfig.class,
            NeverSnapshotPolicyConfig.class)
        .run(
            ctx -> {
              ctx.getBean(VirtualThreadCommandBus.class)
                  .execute(new SnapshotProbeCommand("agg-snap-3"));
              assertThat(RecordingEventStoreConfig.LAST_EXPECTED_SNAPSHOT_VERSION.get())
                  .isEqualTo(EventStore.IGNORE_SNAPSHOT);
            });
  }

  record SnapshotProbeCommand(String id) implements Command {}

  record SnapshotProbeState() implements org.streamrune.core.AggregateState {}

  /** Emits nothing, so the probe dispatch resolves entirely inside the load path. */
  static final class SnapshotProbeDecider
      implements org.streamrune.core.Decider<
          SnapshotProbeCommand, SnapshotProbeState, org.streamrune.core.DomainEvent> {
    @Override
    public SnapshotProbeState initialState() {
      return new SnapshotProbeState();
    }

    @Override
    public java.util.List<org.streamrune.core.DomainEvent> decide(
        SnapshotProbeCommand command, SnapshotProbeState state) {
      return java.util.List.of();
    }

    @Override
    public SnapshotProbeState evolve(
        SnapshotProbeState state, org.streamrune.core.DomainEvent event) {
      return state;
    }
  }

  @Configuration
  static class SnapshotProbeDeciderConfig {
    @Bean
    org.streamrune.runtime.DeciderRegistration<
            SnapshotProbeCommand, SnapshotProbeState, org.streamrune.core.DomainEvent>
        snapshotProbeRegistration() {
      return new org.streamrune.runtime.DeciderRegistration<>(
          org.streamrune.core.types.AggregateType.of("test"),
          SnapshotProbeCommand.class,
          cmd -> AggregateId.of(cmd.id()),
          new SnapshotProbeDecider());
    }
  }

  @Configuration
  static class RecordingEventStoreConfig {
    static final java.util.concurrent.atomic.AtomicInteger LAST_EXPECTED_SNAPSHOT_VERSION =
        new java.util.concurrent.atomic.AtomicInteger(-1);

    @Bean
    EventStore recordingEventStore() {
      EventStore store = mock(EventStore.class);
      when(store.load(any(), anyInt()))
          .thenAnswer(
              invocation -> {
                LAST_EXPECTED_SNAPSHOT_VERSION.set(invocation.getArgument(1));
                return org.streamrune.core.AggregateHistory.empty();
              });
      return store;
    }
  }

  @Configuration
  static class BumpedSnapshotPolicyConfig {
    @Bean
    org.streamrune.core.SnapshotPolicy snapshotPolicy() {
      return org.streamrune.core.SnapshotPolicy.everyNEvents(100, 2);
    }
  }

  @Configuration
  static class NeverSnapshotPolicyConfig {
    @Bean
    org.streamrune.core.SnapshotPolicy snapshotPolicy() {
      return org.streamrune.core.SnapshotPolicy.never();
    }
  }

  @Configuration
  static class StubDataSourceConfig {
    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }
  }

  // Provide an EventStore factory directly so we don't need a working PostgresEventStoreFactory
  @Configuration
  static class StubEventStoreConfig {
    @Bean
    EventStoreFactory eventStoreFactory() {
      return SpringTestMocks::emptyEventStore;
    }
  }

  @Configuration
  static class CustomLockerConfig {
    @Bean
    AggregateLocker aggregateLocker() {
      return new CustomLocker();
    }
  }

  static class CustomLocker implements AggregateLocker {
    @Override
    public AutoCloseable acquireLock(
        org.streamrune.core.types.StreamId streamId, java.time.Duration timeout) {
      return () -> {};
    }
  }

  @Test
  void authorizationInterceptorNotRegisteredWithoutPolicy() {
    contextRunner.run(
        ctx -> {
          assertThat(ctx)
              .doesNotHaveBean(org.streamrune.runtime.AuthorizationCommandInterceptor.class);
        });
  }

  @Test
  void authorizationInterceptorRegisteredWhenPolicyPresent() {
    contextRunner
        .withUserConfiguration(StubAuthorizationPolicyConfig.class)
        .run(
            ctx -> {
              assertThat(ctx)
                  .hasSingleBean(org.streamrune.runtime.AuthorizationCommandInterceptor.class);
            });
  }

  @Configuration
  static class StubAuthorizationPolicyConfig {
    @Bean
    org.streamrune.core.CommandAuthorizationPolicy commandAuthorizationPolicy() {
      return org.streamrune.core.CommandAuthorizationPolicy.allowAll();
    }
  }

  @Test
  void auditStoreIsRegistered() {
    contextRunner.run(
        ctx -> {
          assertThat(ctx).hasSingleBean(org.streamrune.core.audit.AuditStore.class);
          assertThat(ctx.getBean(org.streamrune.core.audit.AuditStore.class))
              .isInstanceOf(org.streamrune.postgres.PostgresAuditStore.class);
        });
  }

  @Test
  void auditCommandInterceptorIsRegistered() {
    contextRunner.run(
        ctx -> {
          assertThat(ctx).hasSingleBean(org.streamrune.runtime.AuditCommandInterceptor.class);
        });
  }

  @Test
  void registersAnnotationAuthInterceptorWhenSecurityOnClasspath() {
    contextRunner.run(
        ctx -> {
          assertThat(ctx).hasSingleBean(SpringSecurityUserRoleResolver.class);
          assertThat(ctx).hasSingleBean(AnnotationAuthorizationInterceptor.class);
        });
  }

  @Test
  void registersAuthorizationValidator() {
    contextRunner.run(ctx -> assertThat(ctx).hasSingleBean(StreamRuneAuthorizationValidator.class));
  }

  @Test
  void annotationAuthorizationInterceptorDoesNotCacheResolvedAuthorities() {
    // A cache keyed by UserId would bypass the resolver's principal-match check: one principal's
    // authorities could be served to a different caller presenting the same X-User-Id within the
    // TTL. Every authorization decision must consult the resolver.
    contextRunner
        .withUserConfiguration(CountingResolverConfig.class)
        .run(
            ctx -> {
              var interceptor = ctx.getBean(AnnotationAuthorizationInterceptor.class);
              var resolver = ctx.getBean(CountingResolver.class);

              var requestContext =
                  new org.streamrune.core.StreamRuneContext.RequestContext(
                      org.streamrune.core.types.TraceId.of("t-1"),
                      org.streamrune.core.types.UserId.of("alice"),
                      org.streamrune.core.types.CorrelationId.of("c-1"),
                      java.time.Instant.now(),
                      java.util.Map.of());

              ScopedValue.where(org.streamrune.core.StreamRuneContext.CURRENT, requestContext)
                  .run(
                      () -> {
                        interceptor.before(commandContext(new GuardedCommand("a-1")));
                        interceptor.before(commandContext(new GuardedCommand("a-2")));
                      });

              assertThat(resolver.invocations.get()).isEqualTo(2);
            });
  }

  private static org.streamrune.core.CommandInterceptor.CommandContext commandContext(Command cmd) {
    return new org.streamrune.core.CommandInterceptor.CommandContext(
        cmd,
        cmd.getClass().getSimpleName(),
        org.streamrune.core.types.CommandId.of("cmd-1"),
        org.streamrune.core.types.AggregateType.of("test"),
        AggregateId.of("agg-1"),
        null,
        java.time.Instant.now());
  }

  @Configuration
  static class CountingResolverConfig {
    @Bean
    CountingResolver userRoleResolver() {
      return new CountingResolver();
    }
  }

  static class CountingResolver implements org.streamrune.core.UserRoleResolver {
    final java.util.concurrent.atomic.AtomicInteger invocations =
        new java.util.concurrent.atomic.AtomicInteger();

    @Override
    public org.streamrune.core.UserAuthority resolve(org.streamrune.core.types.UserId userId) {
      invocations.incrementAndGet();
      return new org.streamrune.core.UserAuthority(java.util.Set.of("ADMIN"), java.util.Set.of());
    }
  }

  @Test
  void failsStartupWhenAnnotatedCommandHasNoEnforcement() {
    // Hiding Spring Security removes the default UserRoleResolver, so no
    // AnnotationAuthorizationInterceptor exists — annotated commands must fail startup, not
    // silently execute unguarded. The command bus refuses itself while it is built.
    contextRunner
        .withClassLoader(
            new org.springframework.boot.test.context.FilteredClassLoader(
                org.springframework.security.core.context.SecurityContextHolder.class))
        .withUserConfiguration(AnnotatedDeciderConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .rootCause()
                  .isInstanceOf(IllegalStateException.class)
                  .hasMessageContaining("UNGUARDED")
                  .hasMessageContaining(GuardedCommand.class.getName());
            });
  }

  @Test
  void startsWhenAnnotatedCommandAndEnforcementPresent() {
    // Spring Security is on the test classpath → default resolver + enforcement interceptor exist
    contextRunner
        .withUserConfiguration(AnnotatedDeciderConfig.class)
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }

  @org.streamrune.core.RequireRole("ADMIN")
  record GuardedCommand(String id) implements Command {}

  record GuardedState() implements org.streamrune.core.AggregateState {}

  record GuardedEvent() implements org.streamrune.core.DomainEvent {}

  @Configuration
  static class AnnotatedDeciderConfig {
    @Bean
    org.streamrune.runtime.DeciderRegistration<GuardedCommand, GuardedState, GuardedEvent>
        guardedDeciderRegistration() {
      return new org.streamrune.runtime.DeciderRegistration<>(
          org.streamrune.core.types.AggregateType.of("test"),
          GuardedCommand.class,
          cmd -> AggregateId.of(cmd.id()),
          new org.streamrune.core.Decider<>() {
            @Override
            public GuardedState initialState() {
              return new GuardedState();
            }

            @Override
            public java.util.List<GuardedEvent> decide(GuardedCommand command, GuardedState state) {
              return java.util.List.of(new GuardedEvent());
            }

            @Override
            public GuardedState evolve(GuardedState state, GuardedEvent event) {
              return state;
            }
          });
    }
  }

  @Test
  void asyncCommandBusBeanIsRegistered() {
    contextRunner.run(
        ctx -> {
          assertThat(ctx).hasSingleBean(org.streamrune.core.AsyncCommandBus.class);
          assertThat(ctx.getBean(org.streamrune.core.AsyncCommandBus.class))
              .isSameAs(ctx.getBean(VirtualThreadCommandBus.class));
        });
  }

  @Test
  void scopedValueFilterIsRegisteredWithExplicitOrder() {
    webContextRunner.run(
        ctx -> {
          var registration =
              (org.springframework.boot.web.servlet.FilterRegistrationBean<?>)
                  ctx.getBean("scopedValueFilterRegistration");
          assertThat(registration.getOrder())
              .isEqualTo(StreamRuneAutoConfiguration.SCOPED_VALUE_FILTER_ORDER);
          // Must run AFTER Spring Security's filter chain (default order -100) so the
          // SecurityContextHolder is populated when the filter derives the authorization identity
          // from the authenticated principal — a bare Filter bean would be auto-registered at
          // Ordered.LOWEST_PRECEDENCE with an undefined position.
          assertThat(registration.getOrder()).isGreaterThan(-100);
          assertThat(registration.getFilter()).isSameAs(ctx.getBean(ScopedValueFilter.class));
          assertThat(registration.getUrlPatterns()).containsExactly("/*");
        });
  }

  @Test
  void userDefinedScopedValueFilterRegistrationBacksOffAutoConfiguredOne() {
    webContextRunner
        .withUserConfiguration(CustomFilterRegistrationConfig.class)
        .run(
            ctx -> {
              var registration =
                  (org.springframework.boot.web.servlet.FilterRegistrationBean<?>)
                      ctx.getBean("scopedValueFilterRegistration");
              assertThat(registration.getOrder()).isEqualTo(42);
            });
  }

  @Configuration
  static class CustomFilterRegistrationConfig {
    @Bean
    org.springframework.boot.web.servlet.FilterRegistrationBean<ScopedValueFilter>
        scopedValueFilterRegistration(ScopedValueFilter filter) {
      var registration = new org.springframework.boot.web.servlet.FilterRegistrationBean<>(filter);
      registration.setOrder(42);
      return registration;
    }
  }

  @Test
  void auditBeansNotRegisteredWithoutDataSource() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withUserConfiguration(StubEventStoreConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).doesNotHaveBean(org.streamrune.core.audit.AuditStore.class);
              assertThat(ctx).doesNotHaveBean(org.streamrune.runtime.AuditCommandInterceptor.class);
            });
  }
}
