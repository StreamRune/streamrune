package org.streamrune.runtime;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.lang.management.ManagementFactory;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Refuses to start StreamRune on a JVM that runs the Graal JIT.
 *
 * <p>On JDK 25 the Graal JIT is not reliable with StreamRune — verified on GraalVM CE 25.0.1,
 * GraalVM CE 25.0.5 and Oracle GraalVM 25.0.4, where it is the default compiler — so StreamRune
 * does not support it and does not start on such a JVM unless the JVMCI compiler is off: {@code
 * -XX:+UnlockExperimentalVMOptions -XX:-UseJVMCICompiler}, with which GraalVM compiles with C2, and
 * which every HotSpot JDK 25 accepts. JVMs with C2 (Temurin, Zulu, or GraalVM with the JVMCI
 * compiler off) and native images were unaffected in our runs.
 *
 * <p>The guard reads the JVM's options through its management interface (the module {@code
 * jdk.management}): a JVM whose {@code UseJVMCICompiler} is {@code true} is refused. A later
 * feature release with the Graal JIT is refused the same way, as not verified. A JVM without the
 * JVMCI compiler and a native image are not checked.
 *
 * <p>A JVM whose options cannot be read — a runtime built without {@code jdk.management} — is
 * judged by what every JVM states: one that identifies as a GraalVM (JVMCI) build, by its {@code
 * java.vm.version}, {@code java.vendor.version} or {@code java.vm.name}, or by a {@code
 * jdk.graal.*}, {@code graal.*} or {@code jvmci.*} system property, is refused, because the guard
 * cannot verify that the JVMCI compiler is off; any other starts with a {@code WARN} line.
 *
 * <p>{@link VirtualThreadCommandBus.Builder#build()} runs it, so every application that builds a
 * command bus — the three integrations, which also run it when they start, the {@link StreamRune}
 * facade and a runtime built by hand — goes through it. {@value #ALLOW_PROPERTY}{@code =true} (a
 * configuration key of the integrations, a system property of a runtime built by hand, or {@link
 * VirtualThreadCommandBus.Builder#allowUnreliableScopedValues(boolean)}) starts a refused JVM
 * anyway, at the application's own risk, with one {@code ERROR} line.
 */
public final class ScopedValueJvmGuard {

  /**
   * The opt-out: {@code true} starts StreamRune on a JVM the guard refuses, with an {@code ERROR}
   * line. The same name is the configuration key of the three integrations and the system property
   * a runtime built by hand reads.
   */
  public static final String ALLOW_PROPERTY = "streamrune.jvm.allow-unreliable-scoped-values";

  private static final Logger LOG = LoggerFactory.getLogger(ScopedValueJvmGuard.class);

  /**
   * The flags that switch the JVMCI compiler off, with which GraalVM compiles with C2. A JVM
   * without the JVMCI compiler (Temurin, Zulu) accepts {@code -XX:-UseJVMCICompiler} only after
   * {@code -XX:+UnlockExperimentalVMOptions}, so the pair starts every HotSpot JDK 25.
   */
  private static final String JVMCI_COMPILER_OFF =
      "-XX:+UnlockExperimentalVMOptions -XX:-UseJVMCICompiler";

  /** The system properties whose value names a JVMCI build. */
  private static final Set<String> IDENTIFYING_PROPERTIES =
      Set.of("java.vm.version", "java.vendor.version", "java.vm.name");

  /** The prefixes of the system properties that configure the Graal JIT or JVMCI. */
  private static final Set<String> JVMCI_PROPERTY_PREFIXES =
      Set.of("jdk.graal.", "graal.", "jvmci.");

  /** Whether the opt-out's {@code ERROR} line was logged in this JVM. */
  private static final AtomicBoolean OPT_OUT_LOGGED = new AtomicBoolean();

  private ScopedValueJvmGuard() {}

  /**
   * Checks the running JVM.
   *
   * @param allowUnreliable {@code true} to start on a JVM the guard refuses, with one {@code ERROR}
   *     line per JVM ({@value #ALLOW_PROPERTY})
   * @throws IllegalStateException if the JVM runs the Graal JIT, or is a GraalVM build whose
   *     options cannot be read, and {@code allowUnreliable} is {@code false}; the message names
   *     {@code -XX:+UnlockExperimentalVMOptions -XX:-UseJVMCICompiler} and the opt-out
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
              "StreamRune could not read the compiler options of this JVM, which does not identify"
                  + " as a GraalVM build. On JDK 25 the Graal JIT is not reliable with StreamRune;"
                  + " if this JVM runs it, start it with {}.",
              JVMCI_COMPILER_OFF);
      case UNRELIABLE, UNVERIFIED -> {
        if (!allowUnreliable) {
          throw new IllegalStateException(refusal(verdict, vm.featureVersion()));
        }
        if (optOutLogged.compareAndSet(false, true)) {
          LOG.error(optOutLine(verdict), ALLOW_PROPERTY, JVMCI_COMPILER_OFF);
        }
      }
    }
  }

  private static String refusal(Verdict verdict, int featureVersion) {
    String optOut = "To start anyway, at your own risk, set " + ALLOW_PROPERTY + "=true.";
    if (verdict == Verdict.UNVERIFIED) {
      return "StreamRune does not start on this JVM: it identifies as a GraalVM build, and"
          + " StreamRune could not read its compiler options, which it reads through the module"
          + " jdk.management, so it cannot verify that the Graal JIT is off. On JDK 25 the Graal"
          + " JIT is not reliable with StreamRune. Start the JVM with "
          + JVMCI_COMPILER_OFF
          + " and add the module jdk.management to the runtime so that StreamRune can verify it;"
          + " where it cannot be added, set "
          + ALLOW_PROPERTY
          + "=true once the JVMCI compiler is off. "
          + optOut;
    }
    String jvm =
        featureVersion == 25
            ? "it runs JDK 25 with the Graal JIT, which is not reliable with StreamRune"
            : "it runs JDK "
                + featureVersion
                + " with the Graal JIT, which is not verified: on JDK 25 the Graal JIT is not"
                + " reliable with StreamRune";
    return "StreamRune does not start on this JVM: "
        + jvm
        + ". Start the JVM with "
        + JVMCI_COMPILER_OFF
        + ", with which GraalVM compiles with C2. "
        + optOut;
  }

  private static String optOutLine(Verdict verdict) {
    if (verdict == Verdict.UNVERIFIED) {
      return "StreamRune starts on a GraalVM build whose compiler options it could not read (they"
          + " are read through the module jdk.management), because {}=true. Unless the JVM runs"
          + " with {}, the Graal JIT is not reliable with StreamRune.";
    }
    return "StreamRune starts on a JVM that runs the Graal JIT, which is not reliable with"
        + " StreamRune, because {}=true. Start the JVM with {} instead.";
  }

  /** What the guard concludes about a JVM. */
  enum Verdict {
    /** No JVMCI compiler, the JVMCI compiler off, or a native image. */
    RELIABLE,
    /** The Graal JIT. */
    UNRELIABLE,
    /** The options could not be read, and the JVM identifies as a GraalVM build. */
    UNVERIFIED,
    /** The options could not be read, and the JVM does not identify as a GraalVM build. */
    UNKNOWN
  }

  static Verdict assess(Vm vm) {
    if (vm.nativeImage()) {
      return Verdict.RELIABLE;
    }
    Optional<String> jvmciCompiler;
    try {
      jvmciCompiler = vm.option("UseJVMCICompiler");
    } catch (RuntimeException | LinkageError unreadable) {
      return identifiesAsJvmci(vm) ? Verdict.UNVERIFIED : Verdict.UNKNOWN;
    }
    return jvmciCompiler.isPresent() && Boolean.parseBoolean(jvmciCompiler.get().strip())
        ? Verdict.UNRELIABLE
        : Verdict.RELIABLE;
  }

  /**
   * Whether the JVM identifies as a GraalVM (JVMCI) build by what every JVM states without the
   * management interface: {@code jvmci} in {@code java.vm.version}, {@code GraalVM} in {@code
   * java.vendor.version} or {@code java.vm.name}, or a system property that configures the Graal
   * JIT or JVMCI.
   */
  static boolean identifiesAsJvmci(Vm vm) {
    for (String name : IDENTIFYING_PROPERTIES) {
      String value = vm.property(name);
      if (value != null) {
        String lower = value.toLowerCase(Locale.ROOT);
        if (lower.contains("jvmci") || lower.contains("graalvm")) {
          return true;
        }
      }
    }
    return vm.propertyNames().stream()
        .anyMatch(name -> JVMCI_PROPERTY_PREFIXES.stream().anyMatch(name::startsWith));
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
     * @throws RuntimeException or {@link LinkageError} when the JVM's options cannot be read
     */
    Optional<String> option(String name);

    /** The value of a system property, or {@code null}. */
    default String property(String name) {
      return System.getProperty(name);
    }

    /** The names of the system properties. */
    default Set<String> propertyNames() {
      return System.getProperties().stringPropertyNames();
    }

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
