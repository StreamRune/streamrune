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
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

/**
 * {@link ScopedValueJvmGuard} on every kind of JVM it tells apart, through the seam of the VM facts
 * it reads: a JDK 25 with the Graal JIT is refused unless one of the two workarounds is set or the
 * application opts out; every other JVM starts without a line.
 */
class ScopedValueJvmGuardTest {

  private static final String DISABLED_INTRINSICS = "_scopedValueCache,_setScopedValueCache";

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

  /** The VM facts the guard reads, as a test states them. */
  private record FakeVm(int featureVersion, boolean nativeImage, Map<String, String> options)
      implements ScopedValueJvmGuard.Vm {

    @Override
    public Optional<String> option(String name) {
      return Optional.ofNullable(options.get(name));
    }
  }

  private static FakeVm graalJit(Map<String, String> extraOptions) {
    var options = new java.util.HashMap<>(extraOptions);
    options.put("UseJVMCICompiler", "true");
    return new FakeVm(25, false, options);
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
  void jdk25WithTheGraalJit_isRefused_andTheRefusalNamesBothWorkaroundsAndTheOptOut() {
    assertThatThrownBy(() -> check(graalJit(Map.of()), false))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("JDK 25 with the Graal JIT")
        .hasMessageContaining("-XX:-UseJVMCICompiler")
        .hasMessageContaining(
            "-XX:+UnlockDiagnosticVMOptions -XX:DisableIntrinsic=" + DISABLED_INTRINSICS)
        .hasMessageContaining(ScopedValueJvmGuard.ALLOW_PROPERTY + "=true");
  }

  @Test
  void theGraalJitWithBothIntrinsicsDisabled_starts() {
    assertThatCode(() -> check(graalJit(Map.of("DisableIntrinsic", DISABLED_INTRINSICS)), false))
        .doesNotThrowAnyException();
    assertThat(log.list).isEmpty();
  }

  @Test
  void bothIntrinsicsAmongOthers_orOnLinesOfTheirOwn_start() {
    for (String disabled :
        List.of(
            "_hashCode,_setScopedValueCache,_scopedValueCache",
            "_hashCode\n_scopedValueCache,_setScopedValueCache",
            "_scopedValueCache\n_setScopedValueCache")) {
      assertThatCode(() -> check(graalJit(Map.of("DisableIntrinsic", disabled)), false))
          .as(disabled)
          .doesNotThrowAnyException();
    }
  }

  @Test
  void theGraalJitWithOneOfTheTwoIntrinsicsDisabled_isRefused() {
    for (String disabled : List.of("_scopedValueCache", "_setScopedValueCache", "_hashCode")) {
      assertThatThrownBy(() -> check(graalJit(Map.of("DisableIntrinsic", disabled)), false))
          .as(disabled)
          .isInstanceOf(IllegalStateException.class);
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
  void anotherFeatureRelease_isNotChecked() {
    var jdk26 = new FakeVm(26, false, Map.of("UseJVMCICompiler", "true"));
    assertThatCode(() -> check(jdk26, false)).doesNotThrowAnyException();
    assertThat(log.list).isEmpty();
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
                    .contains("-XX:-UseJVMCICompiler")
                    .contains("-XX:DisableIntrinsic=" + DISABLED_INTRINSICS));
  }

  @Test
  void theOptOutOnAnUnaffectedJvm_logsNothing() {
    check(graalJit(Map.of("DisableIntrinsic", DISABLED_INTRINSICS)), true);
    check(new FakeVm(25, false, Map.of()), true);
    assertThat(log.list).isEmpty();
  }

  @Test
  void optionsThatCannotBeRead_startTheJvm_withAWarningThatNamesTheWorkarounds() {
    ScopedValueJvmGuard.Vm unreadable =
        new ScopedValueJvmGuard.Vm() {
          @Override
          public int featureVersion() {
            return 25;
          }

          @Override
          public boolean nativeImage() {
            return false;
          }

          @Override
          public Optional<String> option(String name) {
            throw new UnsupportedOperationException("no management interface");
          }
        };

    assertThatCode(() -> check(unreadable, false)).doesNotThrowAnyException();

    assertThat(lines(Level.WARN))
        .singleElement()
        .satisfies(
            line ->
                assertThat(line)
                    .contains("could not read")
                    .contains("-XX:-UseJVMCICompiler")
                    .contains("-XX:DisableIntrinsic=" + DISABLED_INTRINSICS));
  }

  @Test
  void theRunningJvm_isReadThroughItsManagementInterface() {
    ScopedValueJvmGuard.Vm platform = ScopedValueJvmGuard.Vm.platform();
    assertThat(platform.featureVersion()).isEqualTo(Runtime.version().feature());
    assertThat(platform.nativeImage()).isFalse();
    assertThat(platform.option("NoSuchVmOption")).isEmpty();
  }
}
