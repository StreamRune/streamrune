package org.streamrune.crypto;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.SealedHierarchy;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.Encrypted;

/**
 * Startup guard that fails fast when a registered type (event, state, or command) carries an {@link
 * Encrypted} field but no {@link CryptoEngine} is configured.
 *
 * <p><b>Why this exists:</b> {@link CryptoShreddingModule} only intercepts {@code @Encrypted}
 * fields when it is registered on the event store's {@code ObjectMapper}, which only happens when a
 * {@link CryptoEngine} is wired in. Without this validator, an application that declares an
 * {@code @Encrypted} field but forgets to configure a {@code CryptoEngine} bean gets zero error
 * signal: Jackson serializes the field as an ordinary {@code String}, and personally identifiable
 * information is persisted as plaintext, forever, with nothing to indicate the mistake. Calling
 * {@link #validate(Collection, CryptoEngine)} at application startup — before any event is ever
 * written — converts that silent data-corruption bug into an immediate, actionable failure.
 *
 * <p>This is a startup-time guard only: it does not change serialization behavior. When a {@link
 * CryptoEngine} is configured, {@link CryptoShreddingModule} continues to own encryption exactly as
 * before.
 */
public final class CryptoConfigValidator {

  private static final Logger log = LoggerFactory.getLogger(CryptoConfigValidator.class);

  private CryptoConfigValidator() {}

  /**
   * Scans {@code types} for {@link Encrypted} record components and throws if any are found while
   * {@code engine} is {@code null}.
   *
   * @param types the classes registered with the application — event and state types (typically
   *     {@code EventTypeRegistry.registeredTypes()}) and command types (the {@link
   *     org.streamrune.core.Command} types registered with the command bus); non-record classes and
   *     classes without {@code @Encrypted} components are silently ignored
   * @param engine the configured {@link CryptoEngine}, or {@code null} if none is wired
   * @throws IllegalStateException if {@code engine} is {@code null} and at least one type in {@code
   *     types} has an {@code @Encrypted} field — the message names every offending class and field,
   *     and explains that a {@code CryptoEngine} bean must be configured; or, with or without an
   *     engine, if a sealed type reachable from {@code types} reports no readable permitted
   *     subclasses (a GraalVM native image without reflection metadata for it — see {@link
   *     SealedHierarchy}), which no walk of this class treats as a leaf
   */
  public static void validate(Collection<Class<?>> types, CryptoEngine engine) {
    // Runs FIRST and UNCONDITIONALLY — the property-level-override residual is a
    // plaintext-PII leak that happens precisely when a CryptoEngine IS configured, i.e. on the
    // branch the @Encrypted-without-engine check below returns early from.
    warnOnJacksonOverridesOverEncryptedTypes(types);
    if (engine != null || types == null || types.isEmpty()) {
      return;
    }

    List<String> offenders = new ArrayList<>();
    Set<Class<?>> visited = new HashSet<>();
    for (Class<?> type : types) {
      scan(type, visited, offenders);
    }

    if (offenders.isEmpty()) {
      return;
    }

    throw new IllegalStateException(
        "Found @Encrypted field(s) on registered type(s) with no CryptoEngine"
            + " configured: "
            + String.join(", ", offenders)
            + ". Without a CryptoEngine, these fields would be persisted as PLAINTEXT PII"
            + " instead of being encrypted (CryptoShreddingModule is only wired into the event"
            + " store's ObjectMapper when a CryptoEngine is present). Configure a CryptoEngine"
            + " bean/instance (e.g. FileSystemCryptoEngine, PostgresCryptoEngine, VaultCryptoEngine,"
            + " AwsKmsCryptoEngine, or InMemoryCryptoEngine for tests) and wire it into your event"
            + " store factory (e.g. PostgresEventStoreFactory#cryptoEngine), or remove the"
            + " @Encrypted annotation(s) if encryption is not actually required for these fields.");
  }

  /**
   * Recursively scans {@code type} — and every type reachable from it — for {@link Encrypted}
   * record components. A record is inspected for {@code @Encrypted} components and its component
   * types are followed (including the generic arguments of collections/optionals/maps). Because
   * {@code @Encrypted} lives on a record component but the path from a registered type to that
   * record may cross a NON-record container or an interface-typed component, the scan also descends
   * through a non-record class's declared fields and through a sealed interface/class's permitted
   * subclasses. This mirrors {@link CryptoShreddingModule}'s Jackson serializer modifier, which
   * fires for every bean type Jackson builds regardless of the container's shape, so the startup
   * guard's coverage matches the encryption scope. JDK/platform, primitive and enum types are not
   * descended into; the {@code visited} set bounds the walk (prevents infinite recursion on cyclic
   * graphs and re-reporting a shared nested type).
   */
  private static void scan(Class<?> type, Set<Class<?>> visited, List<String> offenders) {
    if (type == null || !visited.add(type)) {
      return;
    }
    if (type.isRecord()) {
      for (String fieldName : CryptoShreddingModule.findEncryptedFields(type).keySet()) {
        offenders.add(type.getName() + "." + fieldName);
      }
      for (RecordComponent rc : type.getRecordComponents()) {
        scanType(rc.getGenericType(), visited, offenders);
      }
      return;
    }
    // Keep descending through non-record containers and interface-typed components so
    // a nested @Encrypted record is not invisible (which would let its PII be persisted as
    // plaintext with no error signal, since CryptoShreddingModule WOULD have encrypted the very
    // same nested record had an engine been present). JDK/platform, enum and primitive types never
    // hold a user @Encrypted record — skip them so the walk does not over-reach into library
    // internals.
    if (type.isPrimitive() || type.isEnum() || isJdkType(type)) {
      return;
    }
    if (type.isInterface()) {
      scanPermittedSubclasses(type, visited, offenders);
      return;
    }
    if (Modifier.isAbstract(type.getModifiers())) {
      scanPermittedSubclasses(type, visited, offenders);
    }
    for (Field field : type.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) || field.isSynthetic()) {
        continue;
      }
      scanType(field.getGenericType(), visited, offenders);
    }
    scan(type.getSuperclass(), visited, offenders);
  }

  /** Descends into the permitted subtypes of a sealed interface/class (no-op when not sealed). */
  private static void scanPermittedSubclasses(
      Class<?> type, Set<Class<?>> visited, List<String> offenders) {
    for (Class<?> subclass : permittedOrEmpty(type)) {
      scan(subclass, visited, offenders);
    }
  }

  /**
   * Whether {@code type} is a JDK/platform type the scan must not descend into (String, boxed
   * primitives, collections, etc.): they never carry a user {@code @Encrypted} record and walking
   * their fields would over-reach. Bootstrap/platform-loaded classes (null class loader) and the
   * well-known JDK package prefixes are treated as JDK.
   */
  private static boolean isJdkType(Class<?> type) {
    if (type.getClassLoader() == null) {
      return true;
    }
    Package pkg = type.getPackage();
    if (pkg == null) {
      return false;
    }
    String name = pkg.getName();
    return name.startsWith("java.")
        || name.startsWith("javax.")
        || name.startsWith("jakarta.")
        || name.startsWith("sun.")
        || name.startsWith("jdk.")
        || name.startsWith("com.sun.");
  }

  private static void scanType(Type t, Set<Class<?>> visited, List<String> offenders) {
    if (t instanceof Class<?> c) {
      if (c.isArray()) {
        scanType(c.getComponentType(), visited, offenders);
      } else {
        scan(c, visited, offenders);
      }
    } else if (t instanceof ParameterizedType pt) {
      scanType(pt.getRawType(), visited, offenders);
      for (Type arg : pt.getActualTypeArguments()) {
        scanType(arg, visited, offenders);
      }
    }
    // WildcardType / GenericArrayType / TypeVariable: best-effort skip (rarely serialized as
    // beans).
  }

  // ── Property-level Jackson overrides over an @Encrypted-bearing type ───────────────
  //
  // A property-level @JsonSerialize(using|contentUsing|converter|contentConverter) — or the
  // matching @JsonDeserialize — on a component whose type carries @Encrypted REPLACES the
  // serializer CryptoShreddingModule installed for that type on that one property. No
  // BeanSerializerModifier hook fires for the nested record, so the encrypting property writers
  // never run and the PII is written as PLAINTEXT into the append-only event store. Because no key
  // was ever minted for the subject, a later forget(subjectId) erases nothing: crypto-shredding
  // silently does not apply and a GDPR erasure is reported COMPLETE while the data stays readable
  // forever.
  //
  // Reachable with an ordinary Jackson customization and no warning anywhere in the toolchain:
  //   record Order(String orderId, @JsonSerialize(using = MyCustomerSerializer.class) Customer c)
  // where Customer carries @Encrypted String email. validate() passed at boot because an engine IS
  // present, and the only control was prose in docs/guide/advanced/gdpr-erasure.md.
  //
  // WARN, not a failure. A delegating override that routes back through the default serializer
  // (provider.defaultSerializeValue) still encrypts correctly and is statically indistinguishable
  // from a bypass — CryptoShreddingModuleTest pins that legitimate shape. Failing closed would
  // refuse a correct application; a serializer-time guard would false-positive on the same shape.
  // A boot-time WARN naming the exact component is the strongest signal that cannot be wrong.

  private static final String SERIALIZE_ANNOTATION =
      "com.fasterxml.jackson.databind.annotation.JsonSerialize";
  private static final String DESERIALIZE_ANNOTATION =
      "com.fasterxml.jackson.databind.annotation.JsonDeserialize";

  /** The attributes that REPLACE a serializer/deserializer rather than merely refining typing. */
  private static final List<String> BYPASSING_ATTRIBUTES =
      List.of("using", "contentUsing", "converter", "contentConverter");

  /**
   * Logs a WARN for every registered record component that carries a serializer-replacing
   * {@code @JsonSerialize}/{@code @JsonDeserialize} attribute AND whose (or whose element) type
   * transitively holds an {@link Encrypted} field. Never throws and never changes behaviour: see
   * the note above for why this is a warning.
   *
   * @param types the registered event/state/command types to inspect; {@code null} or empty is a
   *     no-op
   */
  static void warnOnJacksonOverridesOverEncryptedTypes(Collection<Class<?>> types) {
    List<String> offenders = new ArrayList<>();
    collectJacksonOverridesOverEncryptedTypes(types, offenders);
    if (offenders.isEmpty()) {
      return;
    }
    log.warn(
        "Property-level Jackson (de)serializer override(s) on component(s) whose type carries"
            + " @Encrypted: {}. Such an override REPLACES the CryptoShreddingModule serializer for"
            + " that property, so the @Encrypted field is written as PLAINTEXT into the append-only"
            + " event store — no key is minted, so no later forget(subjectId) can reach it and"
            + " crypto-shredding silently does not apply to that data. If the override must stay,"
            + " make it DELEGATE (call provider.defaultSerializeValue(value, gen) rather than"
            + " writing the fields itself), which keeps the encrypting writers in the path. This is"
            + " a warning, not a failure: a delegating override is correct and is statically"
            + " indistinguishable from a bypass (see docs/guide/advanced/gdpr-erasure.md).",
        String.join(", ", offenders));
  }

  /**
   * Collects the offenders {@link #warnOnJacksonOverridesOverEncryptedTypes} would warn about, as
   * {@code DeclaringType#componentName}. Separated from the logging so the detection can be
   * asserted directly rather than through a log appender.
   *
   * @param types the registered types to inspect; {@code null} or empty is a no-op
   * @param offenders collector, appended to in encounter order
   */
  static void collectJacksonOverridesOverEncryptedTypes(
      Collection<Class<?>> types, List<String> offenders) {
    if (types == null || types.isEmpty()) {
      return;
    }
    Set<Class<?>> visited = new HashSet<>();
    for (Class<?> type : types) {
      scanForOverrides(type, visited, offenders);
    }
  }

  /** Mirrors {@link #scan} but looks for override annotations instead of missing engines. */
  private static void scanForOverrides(
      Class<?> type, Set<Class<?>> visited, List<String> offenders) {
    if (type == null || !visited.add(type)) {
      return;
    }
    if (type.isPrimitive() || type.isEnum() || isJdkType(type)) {
      return;
    }
    if (type.isRecord()) {
      for (RecordComponent rc : type.getRecordComponents()) {
        if (hasBypassingOverride(type, rc) && referencesEncrypted(rc.getGenericType())) {
          offenders.add(type.getName() + "#" + rc.getName());
        }
        scanTypeForOverrides(rc.getGenericType(), visited, offenders);
      }
      return;
    }
    if (type.isInterface() || Modifier.isAbstract(type.getModifiers())) {
      for (Class<?> permitted : permittedOrEmpty(type)) {
        scanForOverrides(permitted, visited, offenders);
      }
      if (type.isInterface()) {
        return;
      }
    }
    for (Field field : type.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers())) {
        scanTypeForOverrides(field.getGenericType(), visited, offenders);
      }
    }
    // getDeclaredFields() stops at `type`, so without this the walk cannot reach an
    // override-carrying record that is only referenced from an INHERITED field — exactly the step
    // scan() takes at the same point. Terminates on null (Object's superclass) and on isJdkType.
    scanForOverrides(type.getSuperclass(), visited, offenders);
  }

  private static void scanTypeForOverrides(Type t, Set<Class<?>> visited, List<String> offenders) {
    if (t instanceof Class<?> c) {
      scanForOverrides(c.isArray() ? c.getComponentType() : c, visited, offenders);
    } else if (t instanceof ParameterizedType pt) {
      scanTypeForOverrides(pt.getRawType(), visited, offenders);
      for (Type arg : pt.getActualTypeArguments()) {
        scanTypeForOverrides(arg, visited, offenders);
      }
    }
  }

  /**
   * Whether a record component carries a serializer-REPLACING attribute. The annotation is read off
   * the accessor and the backing field rather than the component: {@code @JsonSerialize} does not
   * target {@code RECORD_COMPONENT}, so javac propagates it to the field and accessor only.
   */
  private static boolean hasBypassingOverride(Class<?> owner, RecordComponent rc) {
    return annotationBypasses(rc.getAccessor().getAnnotations())
        || annotationBypasses(backingFieldAnnotations(owner, rc));
  }

  private static java.lang.annotation.Annotation[] backingFieldAnnotations(
      Class<?> owner, RecordComponent rc) {
    try {
      return owner.getDeclaredField(rc.getName()).getAnnotations();
    } catch (NoSuchFieldException | RuntimeException _) {
      return new java.lang.annotation.Annotation[0];
    }
  }

  /**
   * Whether any of {@code annotations} is a {@code @JsonSerialize}/{@code @JsonDeserialize} with a
   * non-default {@code using}/{@code contentUsing}/{@code converter}/{@code contentConverter}.
   * Reflective by name so crypto-api does not gain a hard dependency on the annotation types being
   * present, and so a plain {@code @JsonSerialize(as = ...)} (typing only, not a replacement) does
   * not trip the warning.
   */
  private static boolean annotationBypasses(java.lang.annotation.Annotation[] annotations) {
    for (java.lang.annotation.Annotation a : annotations) {
      String name = a.annotationType().getName();
      if (!SERIALIZE_ANNOTATION.equals(name) && !DESERIALIZE_ANNOTATION.equals(name)) {
        continue;
      }
      for (String attribute : BYPASSING_ATTRIBUTES) {
        if (isNonDefault(a, attribute)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean isNonDefault(java.lang.annotation.Annotation a, String attribute) {
    try {
      var method = a.annotationType().getMethod(attribute);
      Object value = method.invoke(a);
      return value != null && !value.equals(method.getDefaultValue());
    } catch (ReflectiveOperationException | RuntimeException _) {
      return false; // attribute absent on this Jackson version — not evidence of a bypass
    }
  }

  /**
   * Whether {@code t}, or any type reachable from it, declares an {@link Encrypted} record
   * component. The walk follows record components, generic type arguments, array component types, a
   * sealed type's permitted subclasses, and a non-record class's declared and inherited fields, the
   * same type graph {@link #validate} scans. JDK/platform, primitive and enum types are not
   * descended into, so a component declared as {@code Object} or as a non-sealed interface answers
   * {@code false} whatever it holds at run time.
   *
   * <p>Public for the dead-letter replay's redaction check ({@code DeadLetterRetryRunner}), which
   * uses it to decide whether a record component it cannot read could hold a crypto-shredded value
   * and must therefore fail closed.
   *
   * @param t the declared type to inspect
   * @return {@code true} when an {@code @Encrypted} component is reachable from {@code t}
   * @throws org.streamrune.core.crypto.CryptoMappingException when a reachable record declares a
   *     malformed {@code @Encrypted} component (not a String, or naming a subject component that is
   *     missing or not a String)
   * @throws IllegalStateException when a reachable sealed type reports no readable permitted
   *     subclasses (see {@link SealedHierarchy}); the walk cannot show it free of PII
   */
  public static boolean referencesEncrypted(Type t) {
    return referencesEncrypted(t, new HashSet<>());
  }

  private static boolean referencesEncrypted(Type t, Set<Class<?>> visited) {
    if (t instanceof ParameterizedType pt) {
      if (referencesEncrypted(pt.getRawType(), visited)) {
        return true;
      }
      for (Type arg : pt.getActualTypeArguments()) {
        if (referencesEncrypted(arg, visited)) {
          return true;
        }
      }
      return false;
    }
    if (!(t instanceof Class<?> c)) {
      return false;
    }
    if (c.isArray()) {
      return referencesEncrypted(c.getComponentType(), visited);
    }
    if (c.isPrimitive() || c.isEnum() || isJdkType(c) || !visited.add(c)) {
      return false;
    }
    if (c.isRecord()) {
      if (!CryptoShreddingModule.findEncryptedFields(c).isEmpty()) {
        return true;
      }
      for (RecordComponent rc : c.getRecordComponents()) {
        if (referencesEncrypted(rc.getGenericType(), visited)) {
          return true;
        }
      }
      return false;
    }
    for (Class<?> permitted : permittedOrEmpty(c)) {
      if (referencesEncrypted(permitted, visited)) {
        return true;
      }
    }
    for (Field field : c.getDeclaredFields()) {
      if (!Modifier.isStatic(field.getModifiers())
          && referencesEncrypted(field.getGenericType(), visited)) {
        return true;
      }
    }
    // getDeclaredFields() stops at `c`. An @Encrypted-bearing field INHERITED from a superclass is
    // still serialized by Jackson (and still encrypted by CryptoShreddingModule), and scan() —
    // which this predicate mirrors — already walks the superclass chain and reports it. Without
    // this step the two walks disagree: validate() correctly refuses the type when no engine is
    // configured, while the detector stays SILENT about a property-level override over
    // that same type — the one case the warning exists for. Terminates on null (Object's
    // superclass, which is not a Class) and on isJdkType(Object).
    return referencesEncrypted(c.getSuperclass(), visited);
  }

  /**
   * The permitted subclasses of a sealed type, empty for a type that is not sealed. A sealed type
   * whose permitted subclasses cannot be read — what a GraalVM native image reports for a sealed
   * type not registered for reflection — is refused by {@link SealedHierarchy} instead of being
   * read as "no subtypes": an {@code @Encrypted} field of any subtype would otherwise escape every
   * walk of this class, silently.
   */
  private static List<Class<?>> permittedOrEmpty(Class<?> type) {
    return SealedHierarchy.permittedSubclasses(type, "the @Encrypted startup check");
  }
}
