package org.streamrune.quarkus.graal;

import static org.assertj.core.api.Assertions.assertThat;

import com.oracle.svm.core.annotate.TargetClass;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.flywaydb.core.internal.scanner.LocationScannerCache;
import org.flywaydb.core.internal.scanner.ResourceNameCache;
import org.flywaydb.core.internal.scanner.classpath.ClassPathLocationScanner;
import org.junit.jupiter.api.Test;

/**
 * {@link Target_ClassPathScanner} only works while Flyway still has the members it substitutes and
 * aliases; a Flyway upgrade that renames one would otherwise surface only in a Quarkus native
 * build. The substitution itself runs only in a native image, which the demo's native smoke test
 * builds.
 */
class FlywayClassPathScannerSubstitutionTest {

  private static final String SCANNER =
      "org.flywaydb.core.internal.scanner.classpath.ClassPathScanner";
  private static final String JAR_SCANNER =
      "org.flywaydb.core.internal.scanner.classpath.JarFileClassPathLocationScanner";

  @Test
  void theSubstitutionsTargetFlywaysScanners() {
    assertThat(Target_ClassPathScanner.class.getAnnotation(TargetClass.class).className())
        .isEqualTo(SCANNER);
    assertThat(
            Target_JarFileClassPathLocationScanner.class
                .getAnnotation(TargetClass.class)
                .className())
        .isEqualTo(JAR_SCANNER);
  }

  @Test
  void flywayStillChoosesItsLocationScannerInTheSubstitutedMethod() throws Exception {
    Method method = Class.forName(SCANNER).getDeclaredMethod("createLocationScanner", String.class);

    assertThat(method.getReturnType()).isEqualTo(ClassPathLocationScanner.class);
    assertThat(Modifier.isStatic(method.getModifiers())).isFalse();
  }

  @Test
  void flywayStillHasTheCachesTheSubstitutionFills() throws Exception {
    Class<?> scanner = Class.forName(SCANNER);
    Field locations = scanner.getDeclaredField("locationScannerCache");
    Field resources = scanner.getDeclaredField("resourceNameCache");

    assertThat(locations.getType()).isEqualTo(LocationScannerCache.class);
    assertThat(resources.getType()).isEqualTo(ResourceNameCache.class);
  }

  @Test
  void flywaysJarScannerStillTakesTheSeparator() throws Exception {
    Constructor<?> constructor = Class.forName(JAR_SCANNER).getDeclaredConstructor(String.class);

    assertThat(ClassPathLocationScanner.class.isAssignableFrom(constructor.getDeclaringClass()))
        .isTrue();
  }
}
