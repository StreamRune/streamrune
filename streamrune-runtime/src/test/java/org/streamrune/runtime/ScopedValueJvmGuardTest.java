package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * {@link ScopedValueJvmGuard} on every kind of JVM it tells apart, through the seam of the VM facts
 * it reads: a JDK 25 or later with the Graal JIT is refused unless the JVMCI compiler is off or the
 * application opts out, and so is a GraalVM build whose compiler options cannot be read; every
 * other JVM starts, without a line when its options can be read.
 */
class ScopedValueJvmGuardTest {

  private static final String JVMCI_OFF = "-XX:-UseJVMCICompiler";

  private final ListAppender<ILoggingEvent> log = new ListAppender<>();
  private Logger logger;

  @BeforeEach
  void captureTheLog() {
    logger = (Logger) LoggerFactory.getLogger(ScopedValueJvmGuard.class);
    log.start();
    logger.addAppender(log);
  }

  @AfterEach
  void releaseTheLog() {
    logger.detachAppender(log);
    log.stop();
  }

  /** The system properties of a GraalVM CE 25.0.5 JVM that identify it as a JVMCI build. */
  private static final Map<String, String> GRAALVM_PROPERTIES =
      Map.of(
          "java.vm.version", "25.0.5.0.3+1-jvmci-25.5-b26",
          "java.vendor.version", "GraalVM CE 25.5.5.0.3+1.1",
          "java.vm.name", "OpenJDK 64-Bit Server VM");

  /** The system properties of a Temurin 25 JVM, which is not a JVMCI build. */
  private static final Map<String, String> TEMURIN_PROPERTIES =
      Map.of(
          "java.vm.version", "25.0.5+8-LTS",
          "java.vendor.version", "Temurin-25.0.5+8",
          "java.vm.name", "OpenJDK 64-Bit Server VM");

  /**
   * The VM facts the guard reads, as a test states them; {@code options} is {@code null} for a JVM
   * whose options cannot be read.
   */
  private record FakeVm(
      int featureVersion,
      boolean nativeImage,
      Map<String, String> options,
      Map<String, String> properties)
      implements ScopedValueJvmGuard.Vm {

    FakeVm(int featureVersion, boolean nativeImage, Map<String, String> options) {
      this(featureVersion, nativeImage, options, GRAALVM_PROPERTIES);
    }

    @Override
    public Optional<String> option(String name) {
      if (options == null) {
        throw new NoClassDefFoundError("com/sun/management/HotSpotDiagnosticMXBean");
      }
      return Optional.ofNullable(options.get(name));
    }

    @Override
    public String property(String name) {
      return properties.get(name);
    }

    @Override
    public Set<String> propertyNames() {
      return properties.keySet();
    }
  }

  private static FakeVm graalJit(Map<String, String> extraOptions) {
    var options = new java.util.HashMap<>(extraOptions);
    options.put("UseJVMCICompiler", "true");
    return new FakeVm(25, false, options);
  }

  /** A JVM whose options cannot be read, with these system properties. */
  private static FakeVm unreadable(Map<String, String> properties) {
    return new FakeVm(25, false, null, properties);
  }

  private void check(ScopedValueJvmGuard.Vm vm, boolean allow) {
    ScopedValueJvmGuard.check(allow, vm, new AtomicBoolean());
  }

  private List<String> lines(Level level) {
    return log.list.stream()
        .filter(event -> event.getLevel() == level)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }

  @Test
  void jdk25WithTheGraalJit_isRefused_andTheRefusalNamesTheJvmciCompilerFlagAndTheOptOut() {
    assertThatThrownBy(() -> check(graalJit(Map.of()), false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("JDK 25 with the Graal JIT")
        .hasMessageContaining("not reliable with StreamRune")
        .hasMessageContaining("Start the JVM with " + JVMCI_OFF)
        .hasMessageContaining(ScopedValueJvmGuard.ALLOW_PROPERTY + "=true")
        .hasMessageNotContaining("DisableIntrinsic");
  }

  @Test
  void theGraalJitWithIntrinsicsDisabled_isRefused_whichEverIntrinsicsTheyAre() {
    for (String disabled :
        List.of(
            "_scopedValueCache,_setScopedValueCache",
            "_hashCode,_setScopedValueCache,_scopedValueCache",
            "_scopedValueCache\n_setScopedValueCache",
            "_hashCode")) {
      assertThatThrownBy(() -> check(graalJit(Map.of("DisableIntrinsic", disabled)), false))
          .as(disabled)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(JVMCI_OFF)
          .hasMessageNotContaining("DisableIntrinsic");
    }
  }

  @Test
  void theJvmciCompilerSwitchedOff_starts() {
    var c2 = new FakeVm(25, false, Map.of("UseJVMCICompiler", "false"));
    assertThatCode(() -> check(c2, false)).doesNotThrowAnyException();
    assertThat(log.list).isEmpty();
  }

  @Test
  void aJvmWithoutTheJvmciCompiler_starts() {
    var withoutJvmci = new FakeVm(25, false, Map.of());
    assertThatCode(() -> check(withoutJvmci, false)).doesNotThrowAnyException();
    assertThat(log.list).isEmpty();
  }

  @Test
  void aLaterFeatureReleaseWithTheGraalJit_isRefusedAsNotVerified() {
    var jdk26 = new FakeVm(26, false, Map.of("UseJVMCICompiler", "true"));
    assertThatThrownBy(() -> check(jdk26, false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("JDK 26 with the Graal JIT")
        .hasMessageContaining("not verified")
        .hasMessageContaining(JVMCI_OFF);
    assertThatCode(() -> check(new FakeVm(26, false, Map.of("UseJVMCICompiler", "false")), false))
        .as("the JVMCI compiler off starts it")
        .doesNotThrowAnyException();
  }

  @Test
  void aNativeImage_isNotChecked() {
    var nativeImage = new FakeVm(25, true, Map.of("UseJVMCICompiler", "true"));
    assertThatCode(() -> check(nativeImage, false)).doesNotThrowAnyException();
    assertThat(log.list).isEmpty();
  }

  @Test
  void theOptOut_startsAnAffectedJvm_withOneErrorLinePerJvm() {
    var once = new AtomicBoolean();
    var affected = graalJit(Map.of());

    ScopedValueJvmGuard.check(true, affected, once);
    ScopedValueJvmGuard.check(true, affected, once);

    assertThat(lines(Level.ERROR))
        .singleElement()
        .satisfies(
            line ->
                assertThat(line)
                    .contains(ScopedValueJvmGuard.ALLOW_PROPERTY + "=true")
                    .contains(JVMCI_OFF)
                    .doesNotContain("DisableIntrinsic"));
  }

  @Test
  void theOptOutOnAnUnaffectedJvm_logsNothing() {
    check(new FakeVm(25, false, Map.of("UseJVMCICompiler", "false")), true);
    check(new FakeVm(25, false, Map.of()), true);
    assertThat(log.list).isEmpty();
  }

  @Test
  void optionsThatCannotBeRead_onAJvmciBuild_areRefused_andTheRefusalSaysTheyCouldNotBeVerified() {
    assertThatThrownBy(() -> check(unreadable(GRAALVM_PROPERTIES), false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("could not read")
        .hasMessageContaining("jdk.management")
        .hasMessageContaining(JVMCI_OFF)
        .hasMessageContaining(ScopedValueJvmGuard.ALLOW_PROPERTY + "=true")
        .hasMessageNotContaining("DisableIntrinsic");
  }

  @Test
  void aJvmciBuild_isIdentifiedByItsVmVersion_itsVendorVersion_orAGraalOrJvmciProperty() {
    for (Map<String, String> properties :
        List.of(
            Map.of("java.vm.version", "25.0.1+8-jvmci-b01"),
            Map.of("java.vm.version", "25.0.4.1.1+1-LTS-jvmci-25.4-b23"),
            Map.of("java.vendor.version", "Oracle GraalVM 25.4.4.1.1+1.1"),
            Map.of("java.vm.name", "GraalVM 25"),
            Map.of("jdk.graal.CompilerConfiguration", "economy"),
            Map.of("graal.ShowConfiguration", "info"),
            Map.of("jvmci.Compiler", "graal"))) {
      assertThatThrownBy(() -> check(unreadable(properties), false))
          .as(properties.toString())
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("could not read");
    }
  }

  @Test
  void optionsThatCannotBeRead_onAJvmThatIsNotJvmci_startTheJvm_withAWarningThatNamesTheFlag() {
    for (Map<String, String> properties : List.of(TEMURIN_PROPERTIES, Map.<String, String>of())) {
      log.list.clear();
      assertThatCode(() -> check(unreadable(properties), false))
          .as(properties.toString())
          .doesNotThrowAnyException();
      assertThat(lines(Level.WARN))
          .singleElement()
          .satisfies(
              line ->
                  assertThat(line)
                      .contains("could not read")
                      .contains(JVMCI_OFF)
                      .doesNotContain("DisableIntrinsic"));
    }
  }

  @Test
  void theOptOut_startsAJvmciBuildWhoseOptionsCannotBeRead_withOneErrorLine() {
    var once = new AtomicBoolean();
    var unverified = unreadable(GRAALVM_PROPERTIES);

    ScopedValueJvmGuard.check(true, unverified, once);
    ScopedValueJvmGuard.check(true, unverified, once);

    assertThat(lines(Level.ERROR))
        .singleElement()
        .satisfies(
            line ->
                assertThat(line)
                    .contains("could not read")
                    .contains("jdk.management")
                    .contains(ScopedValueJvmGuard.ALLOW_PROPERTY + "=true")
                    .contains(JVMCI_OFF)
                    .doesNotContain("DisableIntrinsic"));
    assertThat(lines(Level.WARN)).isEmpty();
  }

  @Test
  void aNativeImageWhoseOptionsCannotBeRead_isNotChecked() {
    var nativeImage = new FakeVm(25, true, null, GRAALVM_PROPERTIES);
    assertThatCode(() -> check(nativeImage, false)).doesNotThrowAnyException();
    assertThat(log.list).isEmpty();
  }

  @Test
  void theRunningJvm_isReadThroughItsManagementInterface() {
    ScopedValueJvmGuard.Vm platform = ScopedValueJvmGuard.Vm.platform();
    assertThat(platform.featureVersion()).isEqualTo(Runtime.version().feature());
    assertThat(platform.nativeImage()).isFalse();
    assertThat(platform.option("NoSuchVmOption")).isEmpty();
    assertThat(platform.property("java.vm.version"))
        .isEqualTo(System.getProperty("java.vm.version"));
    assertThat(platform.propertyNames()).contains("java.vm.version");
  }
}
