package org.streamrune.core;

import java.util.List;

/**
 * Reads the permitted subclasses of a sealed type for the framework's startup walks over sealed
 * hierarchies (dead-letter command registration, the authorization check, the {@code @Encrypted}
 * check), and refuses a sealed type whose permitted subclasses cannot be read instead of treating
 * it as a leaf.
 *
 * <p><b>Why a sealed type can be unreadable.</b> A GraalVM native image answers {@link
 * Class#getPermittedSubclasses()} only for a sealed type registered for reflection. For any other
 * sealed type it reports {@link Class#isSealed()} {@code true} and an EMPTY array (measured on
 * GraalVM CE 25.0.1; registering only the subclasses does not help — the sealed type itself must be
 * registered, and every nested sealed level with it). A walk that read that empty array as "no
 * subtypes" would see only the root, silently: a dead-lettered command of a registered root would
 * no longer resolve, an annotated subtype would escape the authorization check, and an {@code
 * Encrypted} field of a subtype would escape the startup guard. The JVM never gives that answer for
 * a class javac compiled (a sealed class must permit at least one subclass), except when every
 * permitted subclass fails to load.
 *
 * <p><b>Registering a sealed type.</b> A plain type entry in the image's reachability metadata
 * ({@code {"type": "com.example.OrderCommand"}} in {@code reachability-metadata.json}) makes the
 * image list the permitted subclasses; Spring's {@code
 * StreamRuneRuntimeHints.registerDomainPackages} emits it for the sealed types it scans. Quarkus'
 * {@code @RegisterForReflection} does not (measured on Quarkus 3.32: it writes a legacy {@code
 * reflect-config.json} entry without {@code allPermittedSubclasses}, and the binary still reported
 * the root with no permitted subclasses). Neither do Micronaut's {@code @TypeHint} and {@code
 * ReflectiveAccess} (measured on Micronaut 4.10: they register the type through {@code
 * RuntimeReflection.register(Class)}, which leaves the permitted subclasses unregistered — the
 * demo's binary still refused, and a standalone image registering a sealed type that way reported
 * it with no permitted subclasses).
 *
 * <p><b>The rule.</b> A type that reports {@code isSealed()} but no permitted subclass is refused
 * with an {@link IllegalStateException} naming the type and the walk. It is decided on the answer
 * alone, never on whether the code runs in a native image, so the JVM and the image behave alike. A
 * native image built with {@code --exact-reachability-metadata} throws its own {@code
 * MissingReflectionRegistrationError} from {@code getPermittedSubclasses()} instead; that error is
 * left to propagate. A type that is not sealed (a record, a final or non-sealed class, a non-sealed
 * interface, an array, a primitive) has no permitted subclasses to walk.
 */
public final class SealedHierarchy {

  private SealedHierarchy() {}

  /**
   * Returns the permitted direct subclasses of {@code type} when it is sealed, or an empty list
   * when it is not.
   *
   * @param type the type to expand
   * @param walk what the caller expands the hierarchy for, named in the refusal (e.g. {@code
   *     "dead-letter command registration"})
   * @return the permitted direct subclasses, in the order {@link Class#getPermittedSubclasses()}
   *     reports them; empty when {@code type} is not sealed
   * @throws IllegalArgumentException if {@code type} or {@code walk} is null
   * @throws IllegalStateException if {@code type} is sealed but none of its permitted subclasses
   *     can be read
   */
  public static List<Class<?>> permittedSubclasses(Class<?> type, String walk) {
    if (type == null) {
      throw new IllegalArgumentException("type is required");
    }
    if (walk == null) {
      throw new IllegalArgumentException("walk is required");
    }
    if (!type.isSealed()) {
      return List.of();
    }
    Class<?>[] permitted = type.getPermittedSubclasses();
    if (permitted == null || permitted.length == 0) {
      throw new IllegalStateException(
          "Sealed type "
              + type.getName()
              + " reports no permitted subclasses, so "
              + walk
              + " cannot see the types it permits and would treat it as a leaf. A GraalVM native"
              + " image reports exactly that for a sealed type that is not registered for"
              + " reflection: register the sealed type itself, and every nested sealed level, for"
              + " reflection in the image. For Spring,"
              + " StreamRuneRuntimeHints.registerDomainPackages registers the sealed types of the"
              + " scanned packages; otherwise add a {\"type\": \""
              + type.getName()
              + "\"} entry to META-INF/native-image/<groupId>/<artifactId>/"
              + "reachability-metadata.json (Quarkus' @RegisterForReflection and Micronaut's"
              + " @TypeHint or @ReflectiveAccess are not enough: they do not register the permitted"
              + " subclasses). Registering only its subclasses is not"
              + " enough either. On the JVM it means none of its permitted subclasses could be"
              + " loaded.");
    }
    return List.of(permitted);
  }
}
