package org.streamrune.runtime;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Refuses to start StreamRune on a JVM on which {@link ScopedValue} reads on virtual threads are
 * not reliable.
 *
 * <p>On JDK 25 with the Graal JIT — verified on GraalVM CE 25.0.1, GraalVM CE 25.0.5 and Oracle
 * GraalVM 25.0.4 — {@code ScopedValue} reads on virtual threads are not reliable. StreamRune passes
 * the request context, the dispatch cause and the saga markers in scoped values ({@code
 * StreamRuneContext}) and runs commands, subscriptions and sagas on virtual threads, so it does not
 * start on such a JVM unless one of these is set:
 *
 * <ul>
 *   <li>{@code -XX:-UseJVMCICompiler}, or
 *   <li>{@code -XX:+UnlockDiagnosticVMOptions
 *       -XX:DisableIntrinsic=_scopedValueCache,_setScopedValueCache}.
 * </ul>
 *
 * <p>JVMs with C2 (Temurin, Zulu, or GraalVM with {@code -XX:-UseJVMCICompiler}) and native images
 * were unaffected in our runs. The guard reads the JVM's options through its management interface:
 * a JDK 25 HotSpot JVM whose {@code UseJVMCICompiler} is {@code true} and whose {@code
 * DisableIntrinsic} does not name both intrinsics above is refused. Another feature release, a JVM
 * without the JVMCI compiler and a native image are not checked; a JVM whose options cannot be read
 * starts with a {@code WARN} line.
 *
 * <p>{@link VirtualThreadCommandBus.Builder#build()} runs it, so every application that builds a
 * command bus — the three integrations, which also run it when they start, the {@link StreamRune}
 * facade and a runtime built by hand — goes through it. {@value #ALLOW_PROPERTY}{@code =true} (a
 * configuration key of the integrations, a system property of a runtime built by hand, or {@link
 * VirtualThreadCommandBus.Builder#allowUnreliableScopedValues(boolean)}) starts such a JVM anyway,
 * at the application's own risk, with one {@code ERROR} line.
 */
public final class ScopedValueJvmGuard {

  /**
   * The opt-out: {@code true} starts StreamRune on a JVM the guard refuses, with an {@code ERROR}
   * line. The same name is the configuration key of the three integrations and the system property
   * a runtime built by hand reads.
   */
  public static final String ALLOW_PROPERTY = "streamrune.jvm.allow-unreliable-scoped-values";

  private static final Logger LOG = LoggerFactory.getLogger(ScopedValueJvmGuard.class);

  private static final Set<String> WORKAROUND_INTRINSICS =
      Set.of("_scopedValueCache", "_setScopedValueCache");

  /** The two workarounds, as the flags of the java command. */
  private static final String WORKAROUND_FLAGS =
      "-XX:-UseJVMCICompiler, or with -XX:+UnlockDiagnosticVMOptions"
          + " -XX:DisableIntrinsic=_scopedValueCache,_setScopedValueCache";

  /** Whether the opt-out's {@code ERROR} line was logged in this JVM. */
  private static final AtomicBoolean OPT_OUT_LOGGED = new AtomicBoolean();

  private ScopedValueJvmGuard() {}

  /**
   * Checks the running JVM.
   *
   * @param allowUnreliable {@code true} to start on a JVM the guard refuses, with one {@code ERROR}
   *     line per JVM ({@value #ALLOW_PROPERTY})
   * @throws IllegalStateException if the JVM runs JDK 25 with the Graal JIT and neither workaround
   *     is set, and {@code allowUnreliable} is {@code false}; the message names both workarounds
   *     and the opt-out
   */
  public static void check(boolean allowUnreliable) {
    check(allowUnreliable, Vm.platform(), OPT_OUT_LOGGED);
  }

  /** {@link #check(boolean)} on the given VM facts; the seam of the tests. */
  static void check(boolean allowUnreliable, Vm vm, AtomicBoolean optOutLogged) {
    Verdict verdict = assess(vm);
    switch (verdict) {
      case RELIABLE -> {
        // nothing to say
      }
      case UNKNOWN ->
          LOG.warn(
              "StreamRune could not read the compiler options of this JVM. On JDK 25 with the"
                  + " Graal JIT, ScopedValue reads on virtual threads are not reliable; if this JVM"
                  + " runs one, start it with {}.",
              WORKAROUND_FLAGS);
      case UNRELIABLE -> {
        if (!allowUnreliable) {
          throw new IllegalStateException(
              "StreamRune does not start on this JVM: it runs JDK 25 with the Graal JIT, on which"
                  + " ScopedValue reads on virtual threads are not reliable, and StreamRune passes"
                  + " the request context, the dispatch cause and the saga markers in ScopedValues"
                  + " on virtual threads. Start the JVM with "
                  + WORKAROUND_FLAGS
                  + ". To start anyway, at your own risk, set "
                  + ALLOW_PROPERTY
                  + "=true.");
        }
        if (optOutLogged.compareAndSet(false, true)) {
          LOG.error(
              "StreamRune starts on a JVM on which ScopedValue reads on virtual threads are not"
                  + " reliable (JDK 25 with the Graal JIT), because {}=true. Start the JVM with {}"
                  + " instead.",
              ALLOW_PROPERTY,
              WORKAROUND_FLAGS);
        }
      }
    }
  }

  /** What the guard concludes about a JVM. */
  enum Verdict {
    /** Another feature release, no JVMCI compiler, a workaround set, or a native image. */
    RELIABLE,
    /** JDK 25 with the Graal JIT and neither workaround. */
    UNRELIABLE,
    /** The JVM's options could not be read. */
    UNKNOWN
  }

  static Verdict assess(Vm vm) {
    if (vm.featureVersion() != 25 || vm.nativeImage()) {
      return Verdict.RELIABLE;
    }
    Optional<String> jvmciCompiler;
    Optional<String> disabledIntrinsics;
    try {
      jvmciCompiler = vm.option("UseJVMCICompiler");
      disabledIntrinsics = vm.option("DisableIntrinsic");
    } catch (RuntimeException | LinkageError unreadable) {
      return Verdict.UNKNOWN;
    }
    if (jvmciCompiler.isEmpty() || !Boolean.parseBoolean(jvmciCompiler.get().strip())) {
      return Verdict.RELIABLE;
    }
    Set<String> disabled =
        Arrays.stream(disabledIntrinsics.orElse("").split("[,\\s]+"))
            .filter(name -> !name.isEmpty())
            .collect(Collectors.toSet());
    return disabled.containsAll(WORKAROUND_INTRINSICS) ? Verdict.RELIABLE : Verdict.UNRELIABLE;
  }

  /** The facts of a JVM the guard reads; {@link #platform()} reads the running one. */
  interface Vm {

    /** The feature release of the JDK, {@code 25} for JDK 25. */
    default int featureVersion() {
      return Runtime.version().feature();
    }

    /** Whether the code runs in, or is being built into, a native image. */
    default boolean nativeImage() {
      return System.getProperty("org.graalvm.nativeimage.imagecode") != null;
    }

    /**
     * The value of a VM option.
     *
     * @param name the option's name
     * @return its value, as the JVM prints it; empty when the JVM has no such option, or does not
     *     show it
     */
    Optional<String> option(String name);

    /** The running JVM, read through {@link HotSpotDiagnosticMXBean}. */
    static Vm platform() {
      return name -> {
        HotSpotDiagnosticMXBean diagnostics =
            ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
        if (diagnostics == null) {
          throw new UnsupportedOperationException("this JVM has no HotSpot diagnostic interface");
        }
        try {
          return Optional.of(diagnostics.getVMOption(name).getValue());
        } catch (IllegalArgumentException noSuchOption) {
          return Optional.empty();
        }
      };
    }
  }
}
