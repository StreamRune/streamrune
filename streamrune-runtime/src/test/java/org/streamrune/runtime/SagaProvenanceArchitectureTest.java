package org.streamrune.runtime;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.AccessTarget;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;

/**
 * The statement "a saga's dispatch caused this event" has one writer in the core and runtime
 * modules. {@link EventMetadata#sagaOrigin()} is the framework's record of the saga that dispatched
 * the command behind an event, so each step that produces it is confined to one class:
 *
 * <ul>
 *   <li>{@link SagaStepExecutor} names the dispatching saga ({@link SagaOrigin#dispatchedBy});
 *   <li>{@link SagaCommandDispatch} binds it as {@link StreamRuneContext#CAUSE}, and {@link
 *       VirtualThreadCommandBus} carries that binding onto its asynchronous thread;
 *   <li>{@link VirtualThreadCommandBus} copies it into the metadata of the events it stores.
 * </ul>
 *
 * <p>A rule matches a call of the guarded method or constructor and a method reference to it
 * ({@code SagaOrigin::dispatchedBy}, {@code SagaOrigin::new}, {@code EventMetadata::new}) alike.
 *
 * <p>Compiled classes of both modules are analysed; the source scan {@code
 * SagaProvenanceWriterRatchetTest} in {@code streamrune-core} covers the main sources of every
 * other module.
 */
@AnalyzeClasses(
    packages = {"org.streamrune.core", "org.streamrune.runtime"},
    importOptions = ImportOption.DoNotIncludeTests.class)
class SagaProvenanceArchitectureTest {

  private static final String CONSTRUCTOR = "<init>";

  @ArchTest
  static final ArchRule onlyTheContextSagaDispatchAndTheBusTouchTheCause =
      noClasses()
          .that()
          .doNotBelongToAnyOf(
              StreamRuneContext.class, SagaCommandDispatch.class, VirtualThreadCommandBus.class)
          .should()
          .accessField(StreamRuneContext.class, "CAUSE");

  @ArchTest
  static final ArchRule onlyTheStepExecutorNamesADispatchingSaga =
      noClasses()
          .that()
          .doNotBelongToAnyOf(SagaStepExecutor.class)
          .should()
          .accessTargetWhere(
              callOrReferenceOf(SagaOrigin.class, "dispatchedBy", SagaType.class, SagaId.class));

  @ArchTest
  static final ArchRule onlySagaOriginCallsItsConstructor =
      noClasses()
          .that()
          .doNotBelongToAnyOf(SagaOrigin.class)
          .should()
          .accessTargetWhere(
              callOrReferenceOf(
                  SagaOrigin.class, CONSTRUCTOR, SagaType.class, SagaId.class, int.class));

  @ArchTest
  static final ArchRule onlySagaDispatchBuildsADispatchCause =
      noClasses()
          .that()
          .doNotBelongToAnyOf(StreamRuneContext.class, SagaCommandDispatch.class)
          .should()
          .accessTargetWhere(
              callOrReferenceOf(
                  StreamRuneContext.DispatchCause.class,
                  CONSTRUCTOR,
                  CausationId.class,
                  SagaOrigin.class));

  @ArchTest
  static final ArchRule onlyTheBusBuildsMetadataWithAnOrigin =
      noClasses()
          .that()
          .doNotBelongToAnyOf(EventMetadata.class, VirtualThreadCommandBus.class)
          .should()
          .accessTargetWhere(
              callOrReferenceOf(
                  EventMetadata.class,
                  CONSTRUCTOR,
                  EventId.class,
                  CommandId.class,
                  TraceId.class,
                  SpanId.class,
                  CorrelationId.class,
                  CausationId.class,
                  UserId.class,
                  Instant.class,
                  Map.class,
                  SagaOrigin.class));

  /**
   * Matches every access whose target is the given method or constructor: a call, and a method
   * reference, which the compiler turns into no call at the place it is written.
   */
  private static DescribedPredicate<JavaAccess<?>> callOrReferenceOf(
      Class<?> owner, String name, Class<?>... parameterTypes) {
    List<String> parameters = Arrays.stream(parameterTypes).map(Class::getName).toList();
    return DescribedPredicate.describe(
        "target is " + owner.getSimpleName() + "." + name + parameters + ", called or referenced",
        access ->
            access.getTarget() instanceof AccessTarget.CodeUnitAccessTarget target
                && target.getOwner().isEquivalentTo(owner)
                && target.getName().equals(name)
                && target.getRawParameterTypes().stream()
                    .map(JavaClass::getName)
                    .toList()
                    .equals(parameters));
  }
}
