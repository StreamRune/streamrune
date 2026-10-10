package org.streamrune.test;

import com.fasterxml.jackson.annotation.JsonValue;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import org.streamrune.core.types.SubjectId;

/**
 * Derives, <strong>from code</strong>, the exact set of StreamRune framework types that must be
 * registered for GraalVM native-image reflection so Jackson can (de)serialise them. Shared by the
 * Spring, Quarkus, and Micronaut native-hint parity tests so all three assert the <em>same</em>
 * code-derived expectation and cannot silently drift when a new value type or a new nested
 * component is added to a serialised DTO.
 *
 * <p>Three independent derivation sources are combined:
 *
 * <ul>
 *   <li>{@link #allFrameworkSerializableTypes()} — every <b>top-level</b> public record and enum
 *       anywhere under {@code org.streamrune.core}, discovered by walking the module's classes
 *       directory or jar. This is the exhaustive source and the only one that does not depend on
 *       what is already registered.
 *   <li>{@link #jsonValueTypesInCorePackage()} — every {@code @JsonValue} value-type record in
 *       {@code org.streamrune.core.types}, discovered by scanning the package on the classpath. Now
 *       a subset of the exhaustive scan; kept because it states the value-type invariant
 *       independently (the exact way {@code SubjectId}/{@code ProjectionName} etc. shipped
 *       unregistered).
 *   <li>{@link #reachableSerializableTypes(Collection)} — the transitive closure of framework
 *       records and enums reachable through the record components (including generic type
 *       arguments) of the registered DTOs, e.g. the {@code SubscriptionLifecycleState} enum nested
 *       in {@code SubscriptionHealth} or the {@code SortDirection} enum nested in {@code Sort}.
 *       Interfaces (the polymorphic event/command/state marker types) and non-framework types are
 *       excluded — applications register their own event and state records. This source reaches
 *       BEYOND {@code org.streamrune.core}: a registered root from any {@code org.streamrune}
 *       module drags its nested framework records/enums in with it.
 * </ul>
 *
 * <p>This deliberately avoids a hard-coded allow-list: the expected set is computed from the actual
 * code, so a registry that omits a reachable type is caught exhaustively.
 *
 * <p>The exhaustive scan is the whole point, and it used to be missing. The expected set was {@code
 * jsonValueTypesInCorePackage() ∪ reachableSerializableTypes(roots)} where the callers pass <em>the
 * registry itself</em> as {@code roots} — so the second source was seeded from the very thing it
 * was checking. A new serialised DTO outside {@code org.streamrune.core.types} that no
 * already-registered type happens to reference was in neither source, {@code missing} came back
 * empty, and all three parity tests passed green while the type shipped unregistered and blew up in
 * a native image on first touch. That was not hypothetical: seven public core records/enums ({@code
 * SagaStatus}, {@code LoadedSaga}, {@code LockMode}, {@code GdprAction}, {@code LateDataPolicy},
 * {@code ProjectionErrorClass}, {@code ProjectionErrorStrategy}) were unregistered in all three
 * registries and no test flagged them. A self-seeded expectation can only ever prove the registry
 * is closed under reachability, never that it is complete.
 *
 * <p>There is deliberately <b>no opt-out list</b> (an explicit, code-reviewed list of types the
 * framework provably never JSON-serialises): an opt-out list is a second registry to keep in sync
 * and it re-admits silence: a type demoted into it is never revisited when it later becomes a DTO.
 * Scoping the exhaustive source to TOP-LEVEL core records/enums achieves the same noise reduction
 * structurally — the SPI result holders an opt-out list would have listed are all nested — and
 * leaves omission as the thing that requires an affirmative decision.
 */
public final class NativeReflectionTypes {

  private static final String CORE_TYPES_PACKAGE = "org.streamrune.core.types";

  /** Root of the exhaustive scan behind {@link #allFrameworkSerializableTypes()}. */
  private static final String CORE_PACKAGE = "org.streamrune.core";

  /**
   * Membership test for the reachability closure. Widened from {@code org.streamrune.core} to all
   * of {@code org.streamrune}, so a registered root outside core drags its nested framework
   * records/enums in with it. The old prefix silently truncated the closure at the core boundary —
   * a serialised record in {@code org.streamrune.runtime} (e.g. {@code ProjectionStatus}, {@code
   * ForgetResult.PurgeOutcome}) was skipped even when it WAS a component of a registered root.
   */
  private static final String FRAMEWORK_PACKAGE_PREFIX = "org.streamrune.";

  private static final String CLASS_SUFFIX = ".class";

  private NativeReflectionTypes() {}

  /**
   * The complete, code-derived set of types every native-hint registry must contain: every
   * {@code @JsonValue} value type plus every framework record/enum reachable from the registered
   * DTO roots.
   *
   * @param registeredRoots the types a framework's registry currently registers (used as walk
   *     roots; nested reachable types are discovered from them)
   * @return the union of the value-type and reachability derivations
   */
  public static Set<Class<?>> expectedNativeReflectionTypes(Collection<Class<?>> registeredRoots) {
    var expected = new LinkedHashSet<Class<?>>();
    expected.addAll(allFrameworkSerializableTypes());
    expected.addAll(jsonValueTypesInCorePackage());
    expected.addAll(reachableSerializableTypes(registeredRoots));
    return expected;
  }

  /**
   * Every <b>top-level</b> public record and enum anywhere under {@code org.streamrune.core}, found
   * by walking the streamrune-core artifact itself (classes directory or jar) rather than by
   * following references out of an existing registry.
   *
   * <p><b>Why top-level only.</b> A top-level public record or enum in the framework's core is a
   * DTO or a domain value by construction: nothing else has a reason to be one. A <em>nested</em>
   * one is usually the opposite — an SPI result or callback holder declared inside the interface
   * that returns it ({@code CommandBus.CommandResult}, {@code EventStore.AppendResult}, {@code
   * OutboxPublisher.BatchResult}, {@code CommandInterceptor.CommandContext}), which never crosses a
   * Jackson boundary. Demanding registration for those would add ~20 entries of pure noise to all
   * three registries and blunt the signal this check exists to give. The nested types that ARE
   * serialised are reached the other way: they are components of a registered DTO, so {@link
   * #reachableSerializableTypes(Collection)} pulls them in — which is exactly how the two nested
   * types already in every registry ({@code Sort.Order}, {@code SubscriptionHealth.Status}) got
   * there. The two sources are complementary, and neither is seeded from the registry's own
   * contents.
   *
   * <p>Anonymous enum-constant bodies ({@code SomeEnum$1}) report neither {@code isEnum()} nor
   * {@code isRecord()} and drop out on their own; non-public types cannot appear in a public
   * signature and are excluded.
   *
   * @return every top-level public framework record/enum under {@code org.streamrune.core}
   */
  public static Set<Class<?>> allFrameworkSerializableTypes() {
    var result = new LinkedHashSet<Class<?>>();
    for (Class<?> type : classesUnderPackage(SubjectId.class, CORE_PACKAGE)) {
      if ((type.isRecord() || type.isEnum())
          && type.getEnclosingClass() == null
          && java.lang.reflect.Modifier.isPublic(type.getModifiers())) {
        result.add(type);
      }
    }
    return result;
  }

  /**
   * Every record in {@code org.streamrune.core.types} that serialises as one JSON value, discovered
   * by scanning the package: its {@code @JsonValue} sits on a component accessor or on a derived
   * method ({@code StreamId.value()}). New value types are included automatically.
   *
   * @return the value-type records, ordered by simple name
   */
  public static Set<Class<?>> jsonValueTypesInCorePackage() {
    var result = new LinkedHashSet<Class<?>>();
    for (Class<?> type : classesInPackage(SubjectId.class, CORE_TYPES_PACKAGE)) {
      if (hasJsonValue(type)) {
        result.add(type);
      }
    }
    return result;
  }

  /**
   * The transitive closure of framework records and enums reachable from the given roots' record
   * components, following generic type arguments (so {@code List<Order>} discovers {@code Order}).
   * Enums are terminal; interfaces and non-framework types are not followed and not included.
   *
   * @param roots the DTO roots to walk from
   * @return every framework record/enum reachable from the roots, including the framework roots
   *     themselves
   */
  public static Set<Class<?>> reachableSerializableTypes(Collection<Class<?>> roots) {
    var found = new LinkedHashSet<Class<?>>();
    Deque<Class<?>> queue = new ArrayDeque<>();
    for (Class<?> root : roots) {
      if (isFrameworkSerializable(root) && found.add(root)) {
        queue.add(root);
      }
    }
    while (!queue.isEmpty()) {
      Class<?> current = queue.poll();
      if (!current.isRecord()) {
        continue; // Enums are terminal — no record components to follow.
      }
      for (RecordComponent component : current.getRecordComponents()) {
        for (Class<?> referenced : classesIn(component.getGenericType())) {
          if (isFrameworkSerializable(referenced) && found.add(referenced)) {
            queue.add(referenced);
          }
        }
      }
    }
    return found;
  }

  /** A framework type that Jackson (de)serialises reflectively: a core record or enum. */
  static boolean isFrameworkSerializable(Class<?> type) {
    return type.getName().startsWith(FRAMEWORK_PACKAGE_PREFIX)
        && (type.isRecord() || type.isEnum());
  }

  /**
   * Whether a record serialises as one JSON value: a public method it declares — its component
   * accessor or a derived method — carries {@code @JsonValue}.
   */
  static boolean hasJsonValue(Class<?> type) {
    if (!type.isRecord()) {
      return false;
    }
    for (java.lang.reflect.Method method : type.getDeclaredMethods()) {
      if (java.lang.reflect.Modifier.isPublic(method.getModifiers())
          && method.isAnnotationPresent(JsonValue.class)) {
        return true;
      }
    }
    return false;
  }

  /** Collects every concrete {@link Class} appearing in a (possibly generic) type. */
  static List<Class<?>> classesIn(Type type) {
    var classes = new ArrayList<Class<?>>();
    collectClasses(type, classes);
    return classes;
  }

  private static void collectClasses(Type type, List<Class<?>> out) {
    switch (type) {
      case Class<?> c -> out.add(c);
      case ParameterizedType p -> {
        collectClasses(p.getRawType(), out);
        for (Type arg : p.getActualTypeArguments()) {
          collectClasses(arg, out);
        }
      }
      default -> {
        // TypeVariable / WildcardType / GenericArrayType: no concrete framework class to register.
      }
    }
  }

  /**
   * Lists the classes declared directly in {@code packageName}. Resolves the package via the anchor
   * class's code source, which Gradle exposes either as the module JAR (module path) or as an
   * exploded classes directory (classpath); both are supported.
   */
  private static List<Class<?>> classesInPackage(Class<?> anchor, String packageName) {
    URL location = anchor.getProtectionDomain().getCodeSource().getLocation();
    Path source;
    try {
      source = Path.of(location.toURI());
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Cannot resolve classpath location: " + location, e);
    }
    String packagePath = packageName.replace('.', '/');
    List<String> simpleNames =
        Files.isDirectory(source)
            ? simpleNamesInDirectory(source.resolve(packagePath))
            : simpleNamesInJar(source, packagePath);
    var classes = new ArrayList<Class<?>>();
    for (String simpleName : simpleNames) {
      classes.add(load(anchor.getClassLoader(), packageName, simpleName));
    }
    return classes;
  }

  /**
   * Lists every class declared in {@code packageName} <em>or any subpackage of it</em>, including
   * nested classes, resolved from the anchor class's code source (module jar or exploded classes
   * directory — Gradle uses either). This is the exhaustive, registry-independent walk behind
   * {@link #allFrameworkSerializableTypes()}.
   */
  private static List<Class<?>> classesUnderPackage(Class<?> anchor, String packageName) {
    Path source = codeSourceOf(anchor);
    String packagePath = packageName.replace('.', '/');
    List<String> binaryNames =
        Files.isDirectory(source)
            ? binaryNamesInDirectory(source, packagePath)
            : binaryNamesInJar(source, packagePath);
    Collections.sort(binaryNames);
    var classes = new ArrayList<Class<?>>();
    for (String binaryName : binaryNames) {
      classes.add(load(anchor.getClassLoader(), binaryName));
    }
    return classes;
  }

  private static List<String> binaryNamesInJar(Path jar, String packagePath) {
    String prefix = packagePath + "/";
    var names = new ArrayList<String>();
    try (JarFile jarFile = new JarFile(jar.toFile())) {
      var entries = jarFile.entries();
      while (entries.hasMoreElements()) {
        String entry = entries.nextElement().getName();
        if (entry.startsWith(prefix) && entry.endsWith(CLASS_SUFFIX)) {
          names.add(entry.substring(0, entry.length() - CLASS_SUFFIX.length()).replace('/', '.'));
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read package " + packagePath + " from " + jar, e);
    }
    return names;
  }

  private static List<String> binaryNamesInDirectory(Path root, String packagePath) {
    Path start = root.resolve(packagePath);
    if (!Files.isDirectory(start)) {
      throw new IllegalStateException("Package directory not found: " + start);
    }
    var names = new ArrayList<String>();
    try (Stream<Path> files = Files.walk(start)) {
      files
          .filter(p -> p.getFileName().toString().endsWith(CLASS_SUFFIX))
          .forEach(
              p -> {
                String relative = root.relativize(p).toString();
                names.add(
                    relative
                        .substring(0, relative.length() - CLASS_SUFFIX.length())
                        .replace(java.io.File.separatorChar, '.')
                        .replace('/', '.'));
              });
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot walk package directory " + start, e);
    }
    return names;
  }

  private static Path codeSourceOf(Class<?> anchor) {
    URL location = anchor.getProtectionDomain().getCodeSource().getLocation();
    try {
      return Path.of(location.toURI());
    } catch (URISyntaxException e) {
      throw new IllegalStateException("Cannot resolve classpath location: " + location, e);
    }
  }

  private static Class<?> load(ClassLoader classLoader, String binaryName) {
    try {
      return Class.forName(binaryName, false, classLoader);
    } catch (ClassNotFoundException | NoClassDefFoundError e) {
      throw new IllegalStateException("Cannot load scanned class " + binaryName, e);
    }
  }

  private static List<String> simpleNamesInJar(Path jar, String packagePath) {
    String prefix = packagePath + "/";
    var names = new ArrayList<String>();
    try (JarFile jarFile = new JarFile(jar.toFile())) {
      var entries = jarFile.entries();
      while (entries.hasMoreElements()) {
        String entry = entries.nextElement().getName();
        if (entry.startsWith(prefix) && entry.endsWith(CLASS_SUFFIX)) {
          String remainder =
              entry.substring(prefix.length(), entry.length() - CLASS_SUFFIX.length());
          // Direct package members only; skip subpackages and nested/synthetic classes.
          if (remainder.indexOf('/') < 0 && remainder.indexOf('$') < 0) {
            names.add(remainder);
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot read package " + packagePath + " from " + jar, e);
    }
    Collections.sort(names);
    return names;
  }

  private static List<String> simpleNamesInDirectory(Path packageDir) {
    var names = new ArrayList<String>();
    try (Stream<Path> files = Files.list(packageDir)) {
      files
          .map(p -> p.getFileName().toString())
          .filter(name -> name.endsWith(CLASS_SUFFIX))
          .map(name -> name.substring(0, name.length() - CLASS_SUFFIX.length()))
          .filter(simpleName -> simpleName.indexOf('$') < 0) // Skip nested/synthetic classes.
          .forEach(names::add);
    } catch (IOException e) {
      throw new UncheckedIOException("Cannot list package directory " + packageDir, e);
    }
    Collections.sort(names);
    return names;
  }

  private static Class<?> load(ClassLoader classLoader, String packageName, String simpleName) {
    String fqn = packageName + "." + simpleName;
    try {
      return Class.forName(fqn, false, classLoader);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Cannot load scanned class " + fqn, e);
    }
  }
}
