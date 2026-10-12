package org.streamrune.runtime;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import java.time.Instant;
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
 * modules. An event's {@link EventMetadata#sagaOrigin()} decides which saga receives the event, so
 * each step that produces it is confined to one class:
 *
 * <ul>
 *   <li>{@link SagaStepExecutor} names the dispatching saga ({@link SagaOrigin#dispatchedBy});
 *   <li>{@link SagaCommandDispatch} binds it as {@link StreamRuneContext#CAUSE}, and {@link
 *       VirtualThreadCommandBus} carries that binding onto its asynchronous thread;
 *   <li>{@link VirtualThreadCommandBus} copies it into the metadata of the events it stores.
 * </ul>
 *
 * <p>Compiled classes of both modules are analysed; the source scan {@code
 * SagaProvenanceWriterRatchetTest} in {@code streamrune-core} covers the main sources of every
 * other module.
 */
@AnalyzeClasses(
    packages = {"org.streamrune.core", "org.streamrune.runtime"},
    importOptions = ImportOption.DoNotIncludeTests.class)
class SagaProvenanceArchitectureTest {

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
          .callMethod(SagaOrigin.class, "dispatchedBy", SagaType.class, SagaId.class);

  @ArchTest
  static final ArchRule onlySagaOriginCallsItsConstructor =
      noClasses()
          .that()
          .doNotBelongToAnyOf(SagaOrigin.class)
          .should()
          .callConstructor(SagaOrigin.class, SagaType.class, SagaId.class, int.class);

  @ArchTest
  static final ArchRule onlyTheBusBuildsMetadataWithAnOrigin =
      noClasses()
          .that()
          .doNotBelongToAnyOf(EventMetadata.class, VirtualThreadCommandBus.class)
          .should()
          .callConstructor(
              EventMetadata.class,
              EventId.class,
              CommandId.class,
              TraceId.class,
              SpanId.class,
              CorrelationId.class,
              CausationId.class,
              UserId.class,
              Instant.class,
              Map.class,
              SagaOrigin.class);
}
