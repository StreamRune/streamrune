package org.streamrune.spring;

import java.lang.reflect.Modifier;
import java.util.List;
import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.LockMode;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.ValidationError;
import org.streamrune.core.Versioned;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.ComplianceReport;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.gdpr.GdprAction;
import org.streamrune.core.gdpr.SubjectExport;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.LateDataPolicy;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeadLetterEntry;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorClass;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.projection.Window;
import org.streamrune.core.projection.WindowResult;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

/**
 * Registers StreamRune core types for GraalVM native image reflection and the Flyway migration
 * scripts schema auto-initialization reads as native image resources, and provides reusable helpers
 * so applications can register their own serialisable types.
 *
 * <p>All value types and core records need constructor and field access for JSON serialisation and
 * ORM mapping when compiled to a native image. This registrar is wired automatically (see {@code
 * StreamRuneAutoConfiguration}) and covers only the framework's own types.
 *
 * <p><strong>Application types must be registered by the application.</strong> StreamRune
 * serialises every domain event (and snapshot state) through Jackson, and its crypto-shredding
 * module inspects each serialised record via {@link Class#getRecordComponents()} to find
 * {@code @Encrypted} components — a call that throws {@code UnsupportedFeatureError} in a native
 * image unless the record's component accessor methods are registered at build time. Spring Boot's
 * AOT engine auto-registers types reachable from {@code @RestController} signatures, but event and
 * snapshot records are reached only dynamically through the event store, so it cannot see them.
 * Register them explicitly with a {@link RuntimeHintsRegistrar} that calls {@link
 * #registerSerializableTypes} or {@link #registerDomainPackages}, wired via
 * {@code @ImportRuntimeHints}:
 *
 * <pre>{@code
 * public class MyAppNativeHints implements RuntimeHintsRegistrar {
 *   @Override
 *   public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
 *     // Option A — scan the application's domain packages (covers new events automatically):
 *     StreamRuneRuntimeHints.registerDomainPackages(hints, classLoader, "com.example.shop.domain");
 *     // Option B — list the types explicitly for full control:
 *     StreamRuneRuntimeHints.registerSerializableTypes(hints, List.of(OrderPlaced.class));
 *   }
 * }
 *
 * @Configuration
 * @ImportRuntimeHints(MyAppNativeHints.class)
 * class MyAppConfig {}
 * }</pre>
 */
public class StreamRuneRuntimeHints implements RuntimeHintsRegistrar {

  /**
   * Reflection member categories required to (de)serialise a type with Jackson in a native image.
   *
   * <p>{@link MemberCategory#INVOKE_DECLARED_CONSTRUCTORS} lets Jackson invoke a record's canonical
   * constructor on read; {@link MemberCategory#INVOKE_DECLARED_METHODS} registers the record
   * component accessors, which is also what makes {@link Class#getRecordComponents()} succeed for
   * the crypto-shredding module; {@link MemberCategory#ACCESS_DECLARED_FIELDS} covers field access.
   */
  private static final MemberCategory[] SERIALIZATION_CATEGORIES = {
    MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
    MemberCategory.INVOKE_DECLARED_METHODS,
    MemberCategory.ACCESS_DECLARED_FIELDS,
  };

  /** All StreamRune types that require reflection hints in a native image. */
  public static final List<Class<?>> NATIVE_IMAGE_TYPES =
      List.of(
          // Value types — every @JsonValue record in org.streamrune.core.types (18)
          AggregateId.class,
          AggregateType.class,
          CausationId.class,
          CommandId.class,
          CorrelationId.class,
          EventId.class,
          EventType.class,
          GlobalOffset.class,
          IdempotencyKey.class,
          ProjectionName.class,
          SagaType.class,
          SpanId.class,
          StreamId.class,
          SubjectId.class,
          SubscriptionName.class,
          TraceId.class,
          UserId.class,
          Version.class,
          // Core records — org.streamrune.core (11)
          AggregateHistory.class,
          DeadLetterRetryPolicy.class,
          EventEnvelope.class,
          EventMetadata.class,
          Page.class,
          PageRequest.class,
          RetryPolicy.class,
          Sort.class,
          UserAuthority.class,
          ValidationError.class,
          Versioned.class,
          // Core nested record + enum — org.streamrune.core (2)
          Sort.Order.class,
          SortDirection.class,
          // Core records — org.streamrune.core.audit (3)
          AuditEntry.class,
          ComplianceReport.class,
          EventAuditEntry.class,
          // Core enum — org.streamrune.core.audit (1)
          AuditOutcome.class,
          // Core records — org.streamrune.core.gdpr (1)
          SubjectExport.class,
          // Core records — org.streamrune.core.outbox (3)
          OutboxEntry.class,
          OutboxEntry.SkipRecord.class,
          OutboxEntryId.class,
          // Core enums — org.streamrune.core.outbox (2)
          OutboxStatus.class,
          OutboxOrderingMode.class,
          // Core records — org.streamrune.core.projection (3)
          ProjectionDeadLetterEntry.class,
          Window.class,
          WindowResult.class,
          // Core records — org.streamrune.core.saga (2)
          SagaCommand.class,
          SagaId.class,
          // Core records — org.streamrune.core.subscription (2)
          SubscriptionConfig.class,
          SubscriptionHealth.class,
          // Core enums — org.streamrune.core.subscription (2)
          SubscriptionHealth.Status.class,
          SubscriptionLifecycleState.class,
          // Top-level core enums/records that the exhaustive classpath scan in
          // NativeReflectionTypes now demands. They were unregistered in all three registries and
          // no test flagged them, because the expected set was seeded from the registry itself;
          // ProjectionDeliveryMode joins them as the mode @ProjectionConfig declares (8)
          LockMode.class,
          GdprAction.class,
          LateDataPolicy.class,
          ProjectionErrorClass.class,
          ProjectionErrorStrategy.class,
          ProjectionDeliveryMode.class,
          LoadedSaga.class,
          SagaStatus.class,
          // Projection types whose process(List, ProjectionRepository) — the default on Projection,
          // the final override on BaseProjection — Projection#writesThroughRepository() resolves
          // with getClass().getMethod(...) when a transactional processor (the starter's
          // JdbcProjectionRepository) is wired. Unregistered, a native image throws
          // NoSuchMethodException there at startup, as the Quarkus and Micronaut binaries did (2)
          Projection.class,
          BaseProjection.class);

  /**
   * Class-path resources the framework reads at startup, as resource patterns.
   *
   * <p>Schema auto-initialization ({@code streamrune.event-store.schema.auto-initialize}, on by
   * default) reads each shipped Flyway migration script by name, and Flyway reads its own {@code
   * version.txt} when it starts. A native image holds only the resources registered at build time:
   * without a script the factory refuses to start, naming it, and without {@code version.txt}
   * Flyway cannot start at all.
   */
  static final List<String> NATIVE_IMAGE_RESOURCES =
      List.of(
          "db/streamrune-migration/*.sql",
          "db/crypto-migration/*.sql",
          "org/flywaydb/core/internal/version.txt");

  @Override
  public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
    registerSerializableTypes(hints, NATIVE_IMAGE_TYPES);
    for (String pattern : NATIVE_IMAGE_RESOURCES) {
      hints.resources().registerPattern(pattern);
    }
  }

  /**
   * Registers reflection hints for application types that StreamRune serialises through Jackson —
   * domain events, snapshot state records, and any value objects nested in them — so they can be
   * (de)serialised in a GraalVM native image, including by the crypto-shredding module's {@link
   * Class#getRecordComponents()} inspection.
   *
   * <p>Call this from an application {@link RuntimeHintsRegistrar} wired via
   * {@code @ImportRuntimeHints}. Registering a type that is never serialised is harmless — it only
   * adds a little reflection metadata to the image. List the sealed types StreamRune walks as well
   * — a sealed command root given to {@code DeadLetterRetryRunner.Builder#registerCommand}, and
   * every nested sealed level below it — or the image cannot list their permitted subclasses and
   * the walk refuses them (see {@link #registerDomainPackages}).
   *
   * @param hints the registry to add reflection hints to
   * @param types the application types to register; may be empty
   */
  public static void registerSerializableTypes(RuntimeHints hints, Iterable<Class<?>> types) {
    for (Class<?> type : types) {
      hints.reflection().registerType(type, SERIALIZATION_CATEGORIES);
    }
  }

  /**
   * Scans the given base packages at build time and registers every concrete type found (records,
   * enums, value objects) for native-image serialisation, and every sealed interface and sealed
   * abstract class so the image can list its permitted subclasses. A convenient alternative to
   * {@link #registerSerializableTypes} that picks up new event/state records and sealed command
   * roots automatically as the domain grows, at the cost of also registering a few never-serialised
   * types (e.g. deciders), which is harmless.
   *
   * <p>Intended to be called from an application {@link RuntimeHintsRegistrar} at AOT processing
   * time, where the application classes are on the build classpath.
   *
   * <p><b>Sealed types.</b> A GraalVM native image answers {@link Class#getPermittedSubclasses()}
   * only for a sealed type that is itself registered for reflection; for any other it reports the
   * type sealed with NO permitted subclasses (registering the subclasses does not help). StreamRune
   * walks sealed hierarchies — {@code DeadLetterRetryRunner.Builder#registerCommand(OrderCommand
   * .class)} registers every command the root permits, and the authorization and {@code Encrypted}
   * startup checks inspect every permitted subtype — and refuses a sealed type it cannot list. So
   * the scan registers each sealed interface and sealed abstract class it finds, nested sealed
   * levels included, as a plain type registration; other interfaces and abstract classes are
   * skipped.
   *
   * @param hints the registry to add reflection hints to
   * @param classLoader the class loader used to resolve scanned class names
   * @param basePackages the application packages to scan, e.g. {@code "com.example.shop.domain"}
   */
  public static void registerDomainPackages(
      RuntimeHints hints, ClassLoader classLoader, String... basePackages) {
    // useDefaultFilters=false + a catch-all include filter + every independent (top-level or static
    // nested) type accepted => concrete types AND interfaces/abstract classes become candidates;
    // the loop below keeps the concrete ones and the sealed ones.
    var scanner =
        new ClassPathScanningCandidateComponentProvider(false) {
          @Override
          protected boolean isCandidateComponent(AnnotatedBeanDefinition beanDefinition) {
            return beanDefinition.getMetadata().isIndependent();
          }
        };
    scanner.addIncludeFilter((metadataReader, metadataReaderFactory) -> true);
    for (String basePackage : basePackages) {
      for (var candidate : scanner.findCandidateComponents(basePackage)) {
        registerCandidate(hints, classLoader, candidate.getBeanClassName());
      }
    }
  }

  /**
   * Registers one scanned type of {@link #registerDomainPackages}: a sealed type for listing its
   * permitted subclasses, a concrete type for serialisation. A name that is absent or cannot be
   * loaded at build time is skipped.
   */
  private static void registerCandidate(
      RuntimeHints hints, ClassLoader classLoader, String className) {
    if (className == null) {
      return;
    }
    Class<?> type;
    try {
      type = Class.forName(className, false, classLoader);
    } catch (ClassNotFoundException | LinkageError _) {
      // A type that cannot be loaded at build time is not needed for native reflection.
      return;
    }
    // Outside the try: a failure to REGISTER a loaded type propagates and fails
    // the AOT build, instead of leaving the type silently out of the reflection hints until it
    // fails at native runtime.
    if (type.isSealed()) {
      // The type registration alone is what makes getPermittedSubclasses() answer in the image; a
      // sealed CLASS also gets the serialisation categories when it is concrete.
      hints.reflection().registerType(type);
    }
    if (!type.isInterface() && !Modifier.isAbstract(type.getModifiers())) {
      hints.reflection().registerType(type, SERIALIZATION_CATEGORIES);
    }
  }
}
