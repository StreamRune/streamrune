package org.streamrune.micronaut;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.fields;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import io.micronaut.context.condition.Condition;

/**
 * ArchUnit rules for streamrune-micronaut module.
 *
 * <p>These rules enforce Micronaut integration patterns.
 */
@AnalyzeClasses(
    packages = "org.streamrune.micronaut",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  // ========== Dependency Rules ==========

  @ArchTest
  static final ArchRule no_other_framework_dependencies =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.micronaut..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("org.springframework..", "io.quarkus..", "org.jboss..");

  // ========== No Business Logic ==========

  @ArchTest
  static final ArchRule no_domain_logic =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.micronaut..")
          .should()
          .haveNameMatching(".*(Decider|Command|Event|Projection)$");

  // ========== Native image ==========

  /**
   * A {@code @Requires(condition = ...)} class is instantiated into the bean definition's
   * compile-time annotation metadata, and a GraalVM native image stores that instance in its image
   * heap, where every object it references must be of a type initialized at build time. A framework
   * type is initialized at run time, so a condition holding one fails the native build (measured
   * with Micronaut 4.10 / GraalVM CE 25.0.1: "An object of type
   * 'org.streamrune.core.ProjectionConfig$Mode' was found in the image heap", reached through
   * {@code ProjectionFactory.HasScheduledProjections}). A condition therefore holds no state; it
   * reads what it needs when {@code matches} runs.
   */
  @ArchTest
  static final ArchRule conditions_hold_no_instance_state =
      fields()
          .that()
          .areDeclaredInClassesThat()
          .implement(Condition.class)
          .should()
          .beStatic()
          .allowEmptyShould(true);
}
