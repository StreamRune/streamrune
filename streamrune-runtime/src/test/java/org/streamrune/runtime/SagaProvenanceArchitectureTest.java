package org.streamrune.runtime;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noCodeUnits;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noMethods;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.AccessTarget;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.domain.JavaClass;
import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.domain.JavaCodeUnit;
import com.tngtech.archunit.core.domain.JavaMethod;
import com.tngtech.archunit.core.domain.JavaModifier;
import com.tngtech.archunit.core.domain.JavaType;
import com.tngtech.archunit.core.domain.ReferencedClassObject;
import com.tngtech.archunit.core.importer.ClassFileImporter;
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
import java.util.Optional;
import java.util.function.Function;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.StreamId;
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
 * <p>The cause's scoped value itself is touched by six code units, named one by one: a method that
 * reads or binds it anywhere else, in one of these classes too, fails {@link
 * #onlyItsSixCodeUnitsTouchTheCause}.
 *
 * <p>A rule matches a call of the guarded method or constructor and a method reference to it
 * ({@code SagaOrigin::dispatchedBy}, {@code SagaOrigin::new}, {@code SagaOrigin::nextHop}, {@code
 * EventMetadata::new}) alike.
 *
 * <p>The reader of that statement is confined too. {@link SagaRunner} routes a non-start event by
 * the origin in its metadata, and asks the application only about an event no saga of its type
 * caused — through a method that receives the stream id and the payload. No method of the saga
 * interfaces or of the runner's builder that routes such an event is handed the envelope or its
 * metadata ({@link #noRoutingHookForNonStartEventsSeesTheEnvelopeOrItsMetadata}), so an application
 * cannot route by a correlation id, a trace id, a user id or baggage: all four are values a request
 * chooses.
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

  /**
   * The code units that touch the cause's scoped value, by owner and name: its declaration, the
   * read in {@code cause()}, the two binders that choose a cause ({@code continuing} and saga
   * dispatch), and the two carriers that take the bound one to another thread ({@code
   * capturedBindings} for {@code wrap}, {@code prepareAsyncTask} for {@code executeAsync}). A new
   * method that reads or binds the cause, in these classes or in any other, is not in the list.
   */
  private static final DescribedPredicate<JavaCodeUnit> MAY_TOUCH_THE_CAUSE =
      DescribedPredicate.describe(
          "one of the six code units that declare, read, bind or carry StreamRuneContext.CAUSE",
          unit ->
              isCodeUnit(unit, StreamRuneContext.class, "<clinit>")
                  || isCodeUnit(unit, StreamRuneContext.class, "cause")
                  || isCodeUnit(unit, StreamRuneContext.class, "continuing", EventEnvelope.class)
                  || isCodeUnit(unit, StreamRuneContext.class, "capturedBindings")
                  || isCodeUnit(
                      unit,
                      SagaCommandDispatch.class,
                      "dispatch",
                      CommandBus.class,
                      SagaOrigin.class,
                      EventId.class,
                      Command.class,
                      IdempotencyKey.class)
                  || (unit.getOwner().isEquivalentTo(VirtualThreadCommandBus.class)
                      && unit.getName().equals("prepareAsyncTask")));

  private static final DescribedPredicate<JavaAccess<?>> THE_CAUSE_FIELD =
      DescribedPredicate.describe(
          "target is the field StreamRuneContext.CAUSE",
          access ->
              access.getTarget() instanceof AccessTarget.FieldAccessTarget field
                  && field.getOwner().isEquivalentTo(StreamRuneContext.class)
                  && field.getName().equals("CAUSE"));

  @ArchTest
  static final ArchRule onlyItsSixCodeUnitsTouchTheCause =
      noCodeUnits().that(not(MAY_TOUCH_THE_CAUSE)).should(access(THE_CAUSE_FIELD));

  /**
   * Guards the list above against going stale: it names six code units, each of them exists and
   * each touches the cause. A unit renamed or emptied of its access would otherwise leave an entry
   * that admits whatever takes its name next.
   */
  @ArchTest
  static void theSixListedCodeUnitsExistAndTouchTheCause(JavaClasses classes) {
    List<String> touching =
        classes.stream()
            .flatMap(javaClass -> javaClass.getCodeUnits().stream())
            .filter(MAY_TOUCH_THE_CAUSE)
            .filter(unit -> unit.getAccessesFromSelf().stream().anyMatch(THE_CAUSE_FIELD))
            .map(JavaCodeUnit::getFullName)
            .sorted()
            .toList();
    assertEquals(
        List.of(
            "org.streamrune.core.StreamRuneContext.<clinit>()",
            "org.streamrune.core.StreamRuneContext.capturedBindings()",
            "org.streamrune.core.StreamRuneContext.cause()",
            "org.streamrune.core.StreamRuneContext.continuing(org.streamrune.core.EventEnvelope)",
            "org.streamrune.runtime.SagaCommandDispatch.dispatch("
                + "org.streamrune.core.CommandBus, org.streamrune.core.saga.SagaOrigin,"
                + " org.streamrune.core.types.EventId, org.streamrune.core.Command,"
                + " org.streamrune.core.types.IdempotencyKey)",
            "org.streamrune.runtime.VirtualThreadCommandBus.prepareAsyncTask("
                + "org.streamrune.core.Command, java.util.concurrent.CompletableFuture,"
                + " org.streamrune.runtime.VirtualThreadCommandBus$AdmissionSlot)"),
        touching);
  }

  /**
   * A dispatch cause obtained without its constructor — read from JSON, built by reflection — needs
   * the class object of the type. No class of the two modules takes it; in particular nothing that
   * replays a stored row does.
   */
  @ArchTest
  static final ArchRule noClassTakesTheClassObjectOfTheDispatchCause =
      noClasses()
          .should(
              new ArchCondition<JavaClass>(
                  "take the class object of StreamRuneContext.DispatchCause") {
                @Override
                public void check(JavaClass javaClass, ConditionEvents events) {
                  boolean found = false;
                  for (ReferencedClassObject reference : javaClass.getReferencedClassObjects()) {
                    if (reference
                        .getRawType()
                        .isEquivalentTo(StreamRuneContext.DispatchCause.class)) {
                      found = true;
                      events.add(
                          SimpleConditionEvent.satisfied(
                              reference,
                              javaClass.getName()
                                  + " takes the class object of DispatchCause in "
                                  + reference.getSourceCodeLocation()));
                    }
                  }
                  if (!found) {
                    events.add(
                        SimpleConditionEvent.violated(
                            javaClass, javaClass.getName() + " takes no such class object"));
                  }
                }
              });

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

  /** The types an application implements or calls to say which saga an event belongs to. */
  private static final List<Class<?>> SAGA_ROUTING_TYPES =
      List.of(
          SagaOrchestrator.class,
          SagaDecider.class,
          SagaDeciderAdapter.class,
          SagaRunner.Builder.class);

  /**
   * A parameter through which a method sees an event's metadata: the envelope or the metadata
   * itself, directly or as a type argument ({@code Function<EventEnvelope, …>}).
   */
  private static boolean seesTheEnvelopeOrItsMetadata(JavaMethod method) {
    for (JavaType parameter : method.getParameterTypes()) {
      for (JavaClass involved : parameter.getAllInvolvedRawTypes()) {
        if (involved.isEquivalentTo(EventEnvelope.class)
            || involved.isEquivalentTo(EventMetadata.class)) {
          return true;
        }
      }
    }
    return false;
  }

  private static final ArchCondition<JavaMethod> SEE_THE_ENVELOPE_OR_ITS_METADATA =
      new ArchCondition<>("have a parameter that involves EventEnvelope or EventMetadata") {
        @Override
        public void check(JavaMethod method, ConditionEvents events) {
          boolean sees = seesTheEnvelopeOrItsMetadata(method);
          events.add(
              new SimpleConditionEvent(
                  method,
                  sees,
                  method.getFullName()
                      + (sees ? " receives" : " does not receive")
                      + " the envelope or its metadata"));
        }
      };

  /**
   * No method named {@code correlate…} of the given types receives the envelope or its metadata.
   */
  private static ArchRule noCorrelatingMethodSeesTheEnvelopeIn(List<Class<?>> types) {
    return noMethods()
        .that()
        .areDeclaredInClassesThat(
            DescribedPredicate.describe(
                "are one of " + types.stream().map(Class::getSimpleName).toList(),
                javaClass -> types.stream().anyMatch(javaClass::isEquivalentTo)))
        .and()
        .haveNameStartingWith("correlate")
        .should(SEE_THE_ENVELOPE_OR_ITS_METADATA);
  }

  /**
   * The application's routing method for an event that is not a start event receives the stream id
   * and the payload. A variant that took the envelope, the metadata, or a function of either would
   * hand the correlation id back to the application as a routing key.
   */
  @ArchTest
  static final ArchRule noRoutingHookForNonStartEventsSeesTheEnvelopeOrItsMetadata =
      noCorrelatingMethodSeesTheEnvelopeIn(SAGA_ROUTING_TYPES);

  /**
   * The rule above goes by name. This one goes by shape: the methods of the routing types that are
   * handed an envelope are exactly the start-event methods and the saga's own logic. A routing
   * method added under another name is not in the list.
   */
  @ArchTest
  static void theMethodsOfTheRoutingTypesThatReceiveAnEnvelopeAreTheListedOnes(
      JavaClasses classes) {
    List<String> receiving =
        classes.stream()
            .filter(c -> SAGA_ROUTING_TYPES.stream().anyMatch(c::isEquivalentTo))
            .flatMap(c -> c.getMethods().stream())
            .filter(method -> !method.getModifiers().contains(JavaModifier.SYNTHETIC))
            .filter(SagaProvenanceArchitectureTest::seesTheEnvelopeOrItsMetadata)
            .map(method -> method.getOwner().getSimpleName() + "." + method.getName())
            .sorted()
            .toList();
    assertEquals(
        List.of(
            "Builder.extractSagaId",
            "Builder.startWhen",
            "SagaDecider.evolve",
            "SagaDecider.handle",
            "SagaDeciderAdapter.evolve",
            "SagaDeciderAdapter.extractSagaId",
            "SagaDeciderAdapter.handle",
            "SagaDeciderAdapter.isStartEvent",
            "SagaOrchestrator.evolve",
            "SagaOrchestrator.extractSagaId",
            "SagaOrchestrator.handle",
            "SagaOrchestrator.isStartEvent"),
        receiving);
  }

  /** Routing methods of the shapes the rule refuses, and one of the shape it admits. */
  interface PlantedRoutingMethods {
    Optional<SagaId> correlateByEnvelope(EventEnvelope event);

    Optional<SagaId> correlateByMetadata(StreamId streamId, EventMetadata metadata);

    void correlateByFunctionOfTheEnvelope(Function<EventEnvelope, Optional<SagaId>> router);
  }

  /** The admitted shape: the stream id and the payload. */
  interface AdmittedRoutingMethod {
    Optional<SagaId> correlateByEventData(StreamId streamId, org.streamrune.core.DomainEvent event);
  }

  /** The rule fails on each planted shape and passes on the admitted one. */
  @ArchTest
  static void theRoutingHookRuleRejectsEachPlantedShape(JavaClasses unused) {
    JavaClasses planted =
        new ClassFileImporter()
            .importClasses(PlantedRoutingMethods.class, AdmittedRoutingMethod.class);

    var refused =
        noCorrelatingMethodSeesTheEnvelopeIn(List.of(PlantedRoutingMethods.class))
            .evaluate(planted);
    assertTrue(refused.hasViolation(), "the planted routing methods must violate the rule");
    String report = String.join("\n", refused.getFailureReport().getDetails());
    for (String method :
        List.of("correlateByEnvelope", "correlateByMetadata", "correlateByFunctionOfTheEnvelope")) {
      assertTrue(report.contains(method), "must reject " + method + ": " + report);
    }

    assertFalse(
        noCorrelatingMethodSeesTheEnvelopeIn(List.of(AdmittedRoutingMethod.class))
            .evaluate(planted)
            .hasViolation(),
        "a routing method of the stream id and the payload must pass");
  }

  private static boolean isCodeUnit(
      JavaCodeUnit unit, Class<?> owner, String name, Class<?>... parameterTypes) {
    return unit.getOwner().isEquivalentTo(owner)
        && unit.getName().equals(name)
        && unit.getRawParameterTypes().stream()
            .map(JavaClass::getName)
            .toList()
            .equals(Arrays.stream(parameterTypes).map(Class::getName).toList());
  }

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
