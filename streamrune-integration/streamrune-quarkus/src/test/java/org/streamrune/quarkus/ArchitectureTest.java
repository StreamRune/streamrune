package org.streamrune.quarkus;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * ArchUnit rules for streamrune-quarkus module.
 *
 * <p>These rules enforce Quarkus integration patterns.
 */
@AnalyzeClasses(
    packages = "org.streamrune.quarkus",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  // ========== Dependency Rules ==========

  @ArchTest
  static final ArchRule no_other_framework_dependencies =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.quarkus..")
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage("org.springframework..", "io.micronaut..", "org.jboss..");

  // ========== No Business Logic ==========

  @ArchTest
  static final ArchRule no_domain_logic =
      noClasses()
          .that()
          .resideInAPackage("org.streamrune.quarkus..")
          .should()
          .haveNameMatching(".*(Decider|Command|Event|Projection)$");
}
