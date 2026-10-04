package org.streamrune.core;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;

/**
 * ArchUnit rules for streamrune-core module.
 *
 * <p>These rules enforce type-safe API and architectural constraints.
 */
@AnalyzeClasses(
    packages = "org.streamrune.core",
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  // ========== Package Structure Rules ==========

  @ArchTest
  static final ArchRule types_package_has_no_core_dependencies =
      classes()
          .that()
          .resideInAPackage("org.streamrune.core.types..")
          .should()
          .onlyDependOnClassesThat()
          .resideInAnyPackage(
              "java..",
              "javax..",
              "com.fasterxml.jackson.annotation..",
              "org.streamrune.core.types..");

  // ========== No Cyclic Dependencies ==========

  @ArchTest
  static final ArchRule no_cyclic_dependencies_between_packages =
      slices().matching("org.streamrune.core.(*)..").should().beFreeOfCycles();
}
