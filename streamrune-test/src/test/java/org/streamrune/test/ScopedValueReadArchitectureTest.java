package org.streamrune.test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.tngtech.archunit.base.DescribedPredicate;
import com.tngtech.archunit.core.domain.JavaAccess;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.streamrune.core.ScopedValueReads;
import org.streamrune.core.StreamRuneContext;

/**
 * No class of the framework but {@link ScopedValueReads} reads a {@link ScopedValue}: none calls or
 * refers to {@code get}, {@code isBound}, {@code orElse} or {@code orElseThrow}. The framework
 * reads the scoped values it binds in one place, so a change to how they are read is a change of
 * that class.
 *
 * <p>The rule is checked on the compiled classes of every module on this module's test classpath —
 * {@code streamrune-core}, {@code streamrune-runtime}, the crypto API and this test kit — and the
 * same rule runs in the Spring, Quarkus and Micronaut integrations and in {@code streamrune-e2e},
 * which together reach every published module.
 */
@AnalyzeClasses(packages = "org.streamrune", importOptions = ImportOption.DoNotIncludeTests.class)
class ScopedValueReadArchitectureTest {

  private static final Set<String> READS = Set.of("get", "isBound", "orElse", "orElseThrow");

  @ArchTest
  static final ArchRule onlyTheReadHelperReadsAScopedValue = onlyTheReadHelperReadsAScopedValue();

  /** The rule, for the modules that check it on their own classpath. */
  static ArchRule onlyTheReadHelperReadsAScopedValue() {
    return noClasses()
        .that()
        .doNotHaveFullyQualifiedName(ScopedValueReads.class.getName())
        .should()
        .accessTargetWhere(
            DescribedPredicate.describe(
                "a read of a ScopedValue (get, isBound, orElse, orElseThrow)",
                (JavaAccess<?> access) ->
                    access.getTarget().getOwner().isEquivalentTo(ScopedValue.class)
                        && READS.contains(access.getTarget().getName())));
  }

  /** A class that reads the request context in each of the four ways, and by reference. */
  static final class ReadsDirectly {
    static Object reads() {
      Supplier<StreamRuneContext.RequestContext> byReference = StreamRuneContext.CURRENT::get;
      return StreamRuneContext.CURRENT.isBound()
          ? StreamRuneContext.CURRENT.get()
          : StreamRuneContext.CAUSE.orElse(null) == null ? byReference : null;
    }
  }

  /** A class that reads through the helper. */
  static final class ReadsThroughTheHelper {
    static Object reads() {
      return ScopedValueReads.valueOrNull(StreamRuneContext.CURRENT);
    }
  }

  @Test
  void theRuleFindsADirectRead_andAReference_andPassesTheHelper() {
    var importer = new ClassFileImporter();
    AssertionError violation =
        assertThrows(
            AssertionError.class,
            () ->
                onlyTheReadHelperReadsAScopedValue()
                    .check(importer.importClasses(ReadsDirectly.class)));
    for (String read :
        List.of(
            "calls method <java.lang.ScopedValue.isBound()>",
            "calls method <java.lang.ScopedValue.get()>",
            "calls method <java.lang.ScopedValue.orElse(java.lang.Object)>",
            "references method <java.lang.ScopedValue.get()>")) {
      assertTrue(
          violation.getMessage().contains(read), () -> read + " in " + violation.getMessage());
    }
    assertDoesNotThrow(
        () ->
            onlyTheReadHelperReadsAScopedValue()
                .check(importer.importClasses(ReadsThroughTheHelper.class)));
  }
}
