package org.streamrune.spring;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * ArchUnit rules for streamrune-spring module.
 *
 * <p>These rules enforce Spring integration patterns.
 */
@AnalyzeClasses(
    packages = "org.streamrune.spring",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  // ========== Dependency Rules ==========

  @ArchTest
  static final ArchRule no_other_framework_dependencies =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.spring..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("io.micronaut..", "io.quarkus..", "org.jboss..");

  // ========== No Business Logic ==========

  @ArchTest
  static final ArchRule no_domain_logic =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.spring..")
          .should()
          .haveNameMatching(".*(Decider|Command|Event|Projection)$");
}
