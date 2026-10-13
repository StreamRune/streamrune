package org.streamrune.runtime;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noCodeUnits;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.AccessTarget;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchCondition;
import com.tngtech.archunit.lang.ArchRule;
import com.tngtech.archunit.lang.ConditionEvents;
import com.tngtech.archunit.lang.SimpleConditionEvent;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.streamrune.core.EventEnvelope;
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
 * The statement "a saga's dispatch caused this event" has its writers named in the core and runtime
 * modules. {@link EventMetadata#sagaOrigin()} is the framework's record of the saga that dispatched
 * the command behind an event, so each step that produces it is confined to one place:
 *
 * <ul>
 *   <li>{@link SagaStepExecutor} names the dispatching saga ({@link SagaOrigin#dispatchedBy});
 *   <li>{@link SagaCommandDispatch} binds it as {@link StreamRuneContext#CAUSE}, and {@link
 *       VirtualThreadCommandBus} carries that binding onto its asynchronous thread;
 *   <li>{@link StreamRuneContext#continuing(EventEnvelope)} binds the cause of a listener's
 *       reaction to a stored event, and alone takes that event's origin one hop further ({@link
 *       SagaOrigin#nextHop()});
 *   <li>{@link VirtualThreadCommandBus} copies the bound cause into the metadata of the events it
 *       stores.
 * </ul>
 *
 * <p>A rule matches a call of the guarded method or constructor and a method reference to it
 * ({@code SagaOrigin::dispatchedBy}, {@code SagaOrigin::new}, {@code SagaOrigin::nextHop}, {@code
 * EventMetadata::new}) alike.
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

  /** The one method of the context that builds a cause and takes an origin one hop further. */
  private static final DescribedPredicate<JavaCodeUnit> CONTINUING =
      DescribedPredicate.describe(
          "StreamRuneContext.continuing(EventEnvelope)",
          unit ->
              unit.getOwner().isEquivalentTo(StreamRuneContext.class)
                  && unit.getName().equals("continuing")
                  && unit.getRawParameterTypes().stream()
                      .map(JavaClass::getName)
                      .toList()
                      .equals(List.of(EventEnvelope.class.getName())));

  private static final DescribedPredicate<JavaCodeUnit> SAGA_DISPATCH =
      DescribedPredicate.describe(
          "a method of SagaCommandDispatch",
          unit -> unit.getOwner().isEquivalentTo(SagaCommandDispatch.class));

  @ArchTest
  static final ArchRule onlySagaDispatchAndContinuingBuildADispatchCause =
      noCodeUnits()
          .that(not(CONTINUING.or(SAGA_DISPATCH)))
          .should(
              access(
                  callOrReferenceOf(
                      StreamRuneContext.DispatchCause.class,
                      CONSTRUCTOR,
                      CausationId.class,
                      SagaOrigin.class)));

  @ArchTest
  static final ArchRule onlyContinuingTakesTheNextHop =
      noCodeUnits()
          .that(not(CONTINUING))
          .should(access(callOrReferenceOf(SagaOrigin.class, "nextHop")));

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
   * A code unit satisfies this when one of its accesses matches {@code target}; under {@code
   * noCodeUnits()} every such access is reported. A lambda's accesses count for the method that
   * declares the lambda.
   */
  private static ArchCondition<JavaCodeUnit> access(DescribedPredicate<JavaAccess<?>> target) {
    return new ArchCondition<>("access where " + target.getDescription()) {
      @Override
      public void check(JavaCodeUnit unit, ConditionEvents events) {
        boolean found = false;
        for (JavaAccess<?> access : unit.getAccessesFromSelf()) {
          if (target.test(access)) {
            found = true;
            events.add(SimpleConditionEvent.satisfied(access, access.getDescription()));
          }
        }
        if (!found) {
          events.add(
              SimpleConditionEvent.violated(unit, unit.getFullName() + " has no such access"));
        }
      }
    };
  }

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
