package org.streamrune.crypto;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.PropertyName;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.deser.std.DelegatingDeserializer;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.introspect.AnnotatedClass;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.introspect.NopAnnotationIntrospector;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializer;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.PropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.ObjectIdWriter;
import com.fasterxml.jackson.databind.ser.impl.UnwrappingBeanSerializer;
import com.fasterxml.jackson.databind.ser.std.BeanSerializerBase;
import com.fasterxml.jackson.databind.util.Converter;
import com.fasterxml.jackson.databind.util.IgnorePropertiesUtil;
import com.fasterxml.jackson.databind.util.NameTransformer;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;

/**
 * Jackson module that intercepts {@link Encrypted} annotated record components during
 * serialization/deserialization, encrypting PII data at rest and decrypting on read.
 *
 * <p>When a key has been deleted (crypto-shredding), deserialization returns {@code "[REDACTED]"}
 * instead of throwing an exception, allowing event replay to continue functioning. Only a
 * definitive {@link KeyNotFoundException} from the engine produces the tombstone; any other failure
 * (transient backend outage, corrupt ciphertext) propagates so replay does not silently persist
 * corrupted state.
 *
 * <p><b>The redact fallback is observable, not silent.</b> A {@link KeyNotFoundException} is raised
 * both for a legitimately crypto-shredded subject <em>and</em> for a whole-store misconfiguration
 * (a DB restored without the key rows, a truncated key table, the engine pointed at the wrong
 * datasource) — in which case <em>every</em> {@code @Encrypted} field on <em>every</em> event
 * decrypts to {@code [REDACTED]} and is written into read models, indistinguishable from lawful
 * erasure. To keep that from being silent, each redact fallback increments the {@code
 * streamrune.crypto.subject_redacted} metric (alert on its rate — a spike is a key-store outage)
 * and logs. Crucially the module distinguishes the two cases: a run of redactions for many
 * <em>distinct</em> subjects with <em>no successful decrypt in between</em> (the signature of a
 * systemic key-store failure, since a genuine forget leaves every other subject decryptable) trips
 * a single {@code ERROR} alarm, while an ordinary per-subject forget only logs a rate-limited
 * {@code WARN}. Wire a real {@link StreamRuneMetrics} via the {@code (CryptoEngine,
 * StreamRuneMetrics)} constructor to feed the metric; the default constructor uses {@link
 * StreamRuneMetrics#NOOP} (the log signal still fires).
 *
 * <p><b>Validation runs on plaintext (decrypt-before-construct):</b> on read this module decrypts
 * each {@code @Encrypted} field in the buffered JSON object <em>before</em> the record's canonical
 * constructor runs, so the compact constructor sees the real plaintext, never the Base64
 * ciphertext. Only the {@code @Encrypted} string values are substituted: every other component
 * reaches the constructor exactly as a direct read of the same JSON would deliver it — a {@code
 * BigDecimal} keeps its scale, an {@code Instant} or {@code Duration} written as {@code
 * seconds.nanos} keeps all nine nanosecond digits — whatever the mapper's own float handling is. An
 * {@code @Encrypted} field may therefore carry ordinary format/parse validation (e.g. "email must
 * contain '@'", a UUID parse, a regex) — it validates the decrypted value exactly as a
 * non-encrypted field would. This is a change from an earlier decrypt-<em>after</em>-construct
 * design in which validating the ciphertext made the record unloadable the moment it was written.
 *
 * <p><b>Post-forget tombstone tolerance (GDPR erasure must keep replay working):</b> one residual,
 * genuinely unavoidable constraint remains. A forgotten subject's {@code @Encrypted} fields decrypt
 * to the {@link #REDACTED} tombstone, and a Java record can never be reconstructed while bypassing
 * its compact-constructor validation (record fields are final and reflection is blocked from
 * mutating them; the canonical constructor always runs the compact body). So a record whose
 * validation would reject {@code "[REDACTED]"} becomes permanently unloadable once its subject is
 * crypto-shredded (erasure turns into a DoS on that stream). Such validation <b>must</b> tolerate
 * the sentinel (skip format/length checks when the value equals {@link #REDACTED}); to make the
 * misconfiguration diagnosable, the reconstruction path otherwise fails with a targeted {@link
 * CryptoMappingException} naming the record, the offending {@code @Encrypted} field(s), and the
 * forgotten subject(s) — rather than an opaque error.
 *
 * <p>Serialization fails fast with {@link CryptoMappingException} when the subjectId component of a
 * non-null {@link Encrypted} field is null — silently writing plaintext PII is never acceptable.
 *
 * <p><b>Every refusal this module raises itself is a {@link CryptoMappingException}</b> (a {@link
 * CryptoOperationException} subtype), and engine failures are <b>never</b> wrapped: a {@code
 * CryptoEngine.encrypt}/{@code decrypt} failure propagates with the engine's own type. That is what
 * lets a caller which must choose between "quarantine this event" and "retry the batch" — {@code
 * SagaRunner} classifying a {@code SagaStateSerializationException}, for one — tell a deterministic
 * mapping refusal (retrying can never succeed) from a transient key-store outage (retrying is the
 * only correct answer) without inspecting messages or stack frames.
 *
 * <p>When stored data carries a {@code null} subjectId at read time (e.g. events written before the
 * type gained its subjectId component, or whose subjectId an upcaster renamed or dropped — this
 * module never writes such data, it fails fast on serialization), the encrypted field keeps its
 * stored value: returning {@link #REDACTED} would falsely signal crypto-shredding and irrecoverably
 * discard data that becomes readable again once the subjectId is restored, and throwing would block
 * replay of every such event.
 *
 * <p><b>Scope of crypto-shredding:</b> deleting a subject's key only affects data that is
 * <em>decrypted after</em> the deletion. Projections and read models that consumed decrypted
 * plaintext before the forget still hold it in their own storage — a GDPR erasure additionally
 * requires purging or rebuilding those downstream copies; this module cannot reach them.
 */
public final class CryptoShreddingModule extends SimpleModule {

  /**
   * Marker value returned when a subject's encryption key has been deleted.
   *
   * <p>The marker is in-band: a legitimate field whose plaintext happens to equal {@code
   * "[REDACTED]"} round-trips unchanged while its key exists, so equality with this constant alone
   * does not prove the subject was shredded. To distinguish definitively, ask the engine — {@link
   * CryptoEngine#isKeyAvailable(SubjectId) isKeyAvailable(subjectId)} returns {@code false} once a
   * subject has been crypto-shredded.
   */
  public static final String REDACTED = "[REDACTED]";

  private static final Logger log = LoggerFactory.getLogger(CryptoShreddingModule.class);

  /**
   * Number of <em>distinct</em> subjects redacted with no successful decrypt in between that trips
   * the systemic-key-store-failure alarm. A genuine per-subject forget never approaches this (every
   * other subject still decrypts, resetting the run), so this cleanly separates lawful erasure from
   * a wiped/misconfigured key store.
   */
  static final int DEFAULT_SYSTEMIC_REDACTION_THRESHOLD = 25;

  private final CryptoEngine cryptoEngine;
  private final StreamRuneMetrics metrics;
  private final int systemicRedactionThreshold;

  /**
   * Distinct subject tokens redacted since the last <em>successful</em> decrypt. A successful
   * decrypt proves the key store is reachable and serving keys, so it clears this run; a long run
   * that only ever grows (no successes) is the signature of a systemic key-store failure.
   */
  private final Set<String> distinctRedactedSinceSuccess = ConcurrentHashMap.newKeySet();

  private final AtomicBoolean systemicAlarmRaised = new AtomicBoolean(false);

  public CryptoShreddingModule(CryptoEngine cryptoEngine) {
    this(cryptoEngine, StreamRuneMetrics.NOOP);
  }

  /**
   * Creates a module that reports every {@code [REDACTED]} fallback to the given metrics collector,
   * so a systemic key-store outage is observable instead of a silent mass-redaction.
   *
   * @param cryptoEngine the engine that encrypts/decrypts {@code @Encrypted} fields; must not be
   *     null
   * @param metrics the metrics collector to report redaction fallbacks to; must not be null (use
   *     {@link StreamRuneMetrics#NOOP} to opt out)
   */
  public CryptoShreddingModule(CryptoEngine cryptoEngine, StreamRuneMetrics metrics) {
    this(cryptoEngine, metrics, DEFAULT_SYSTEMIC_REDACTION_THRESHOLD);
  }

  CryptoShreddingModule(
      CryptoEngine cryptoEngine, StreamRuneMetrics metrics, int systemicRedactionThreshold) {
    if (cryptoEngine == null) throw new IllegalArgumentException("cryptoEngine must not be null");
    if (metrics == null) throw new IllegalArgumentException("metrics must not be null");
    if (systemicRedactionThreshold < 1) {
      throw new IllegalArgumentException("systemicRedactionThreshold must be >= 1");
    }
    super("CryptoShreddingModule");
    this.cryptoEngine = cryptoEngine;
    this.metrics = metrics;
    this.systemicRedactionThreshold = systemicRedactionThreshold;
  }

  @Override
  public void setupModule(SetupContext context) {
    super.setupModule(context);
    context.addBeanSerializerModifier(new EncryptingSerializerModifier());
    context.addBeanDeserializerModifier(new DecryptingDeserializerModifier());
    // Class-level @JsonSerialize(using/converter/as) replaces the
    // bean serializer BEFORE any BeanSerializerModifier hook can fire for the class, so only an
    // annotation-introspection guard can fail those routes closed. INSERTED (primary) so the check
    // runs before the default introspector answers the serializer lookup.
    context.insertAnnotationIntrospector(new EncryptedSerializationOverrideGuard());
    // Class-level
    // @JsonDeserialize(using/converter/builder) makes DeserializerCache._createDeserializer return
    // the override BEFORE the bean-factory path where DecryptingDeserializerModifier hooks, so
    // every read would silently construct records holding raw Base64 ciphertext. The cache
    // consults the introspector pair (inserted = primary first) for findDeserializer /
    // findPOJOBuilder / findDeserializationConverter before any of those routes engage, so this
    // guard fails the read at deserializer construction.
    context.insertAnnotationIntrospector(new EncryptedDeserializationOverrideGuard());
  }

  /**
   * Builds a map of encrypted field name to its subjectId field name for a given record class.
   * Returns an empty map if the class is not a record or has no encrypted components.
   *
   * <p>Validates the {@link Encrypted} mapping eagerly — when Jackson first builds the
   * (de)serializer for the class — so misconfiguration fails fast with a clear message instead of
   * poisoning stored events: a non-String component would serialize fine (via {@code
   * String.valueOf}) but decryption always produces a String, making every subsequent read of that
   * event type fail. A dangling subjectId reference would otherwise only surface on first use.
   *
   * <p>Package-private (not {@code private}) so {@link CryptoConfigValidator} can reuse this exact
   * scan for its startup-time "no CryptoEngine configured" check instead of duplicating the
   * reflection over {@link RecordComponent}s.
   */
  static Map<String, String> findEncryptedFields(Class<?> beanClass) {
    if (!beanClass.isRecord()) return Map.of();
    RecordComponent[] components = beanClass.getRecordComponents();
    Map<String, RecordComponent> componentsByName = new HashMap<>();
    for (RecordComponent rc : components) {
      componentsByName.put(rc.getName(), rc);
    }
    Map<String, String> result = new HashMap<>();
    for (RecordComponent rc : components) {
      Encrypted ann = rc.getAnnotation(Encrypted.class);
      if (ann == null) continue;
      if (rc.getType() != String.class) {
        throw new CryptoMappingException(
            "@Encrypted component '"
                + rc.getName()
                + "' on "
                + beanClass.getName()
                + " must be a String, but is "
                + rc.getType().getName()
                + " — encryption stores a Base64 String and decryption can only restore a String");
      }
      RecordComponent subjectIdComponent = componentsByName.get(ann.subjectId());
      if (subjectIdComponent == null) {
        throw new CryptoMappingException(
            "@Encrypted component '"
                + rc.getName()
                + "' on "
                + beanClass.getName()
                + " references subjectId component '"
                + ann.subjectId()
                + "', which does not exist on the record");
      }
      if (subjectIdComponent.getType() != String.class) {
        // The crypto key subjectId is derived on two DIFFERENT paths that must agree: encrypt uses
        // String.valueOf(subjectIdValue) on the live Java object (EncryptingPropertyWriter),
        // decrypt reads the subjectId property's text off the stored JSON (decryptEncryptedFields).
        // For a String component these are identical, but a non-String component (e.g. a value
        // object serialized as a JSON object) could stringify differently between the two paths,
        // silently deriving a DIFFERENT key on read than was used to encrypt — a permanently
        // undecryptable field with no error, just a wrong key. Fail fast at registration instead of
        // on first read.
        throw new CryptoMappingException(
            "@Encrypted subjectId component '"
                + ann.subjectId()
                + "' on "
                + beanClass.getName()
                + " must be of type String, but is "
                + subjectIdComponent.getType().getName()
                + " — the crypto key subjectId is derived via String.valueOf() on encrypt and"
                + " the stored JSON property's text on decrypt; a non-String type could diverge"
                + " between those two"
                + " paths and silently derive the wrong key, making the encrypted field"
                + " unreadable");
      }
      if (subjectIdComponent.getAnnotation(Encrypted.class) != null) {
        // The subjectId reference is ITSELF @Encrypted. A key-deriving field cannot be
        // encrypted: on encrypt the key would be derived from the subjectId's PLAINTEXT (also
        // minting an encryption_keys row keyed by sha256(that plaintext) that no forget() targeting
        // the real subject id ever deletes — an unshreddable stray key tied to the subject's PII),
        // while on decrypt the subjectId is read off the stored JSON where it is still Base64
        // CIPHERTEXT until its own field decrypts. The two derivations disagree and, worse, which
        // one wins — value readable vs. silently redacted to [REDACTED] — depends on HashMap
        // field-iteration order, so the same record can pass in dev and corrupt in prod. Fail fast
        // at registration, mirroring the non-String-subjectId guard above.
        throw new CryptoMappingException(
            "@Encrypted component '"
                + rc.getName()
                + "' on "
                + beanClass.getName()
                + " references subjectId component '"
                + ann.subjectId()
                + "', which is itself @Encrypted — a key-deriving field cannot be encrypted. The"
                + " crypto key subjectId is derived from the subjectId's PLAINTEXT on encrypt but"
                + " read from the stored JSON (still ciphertext until its own field decrypts) on"
                + " decrypt, so the derived key is order-dependent and the field silently redacts;"
                + " it also mints an encryption_keys row keyed by the subjectId's plaintext that no"
                + " forget() ever deletes. Make the subjectId component a non-@Encrypted field.");
      }
      result.put(rc.getName(), ann.subjectId());
    }
    return result;
  }

  /** Extracts the value of a record component by name via its accessor method. */
  private static Object getRecordComponentValue(Object record, String componentName) {
    try {
      var method = record.getClass().getMethod(componentName);
      method.setAccessible(true);
      return method.invoke(record);
    } catch (Exception e) {
      throw new CryptoMappingException("Failed to read record component: " + componentName, e);
    }
  }

  // ---- Serialization ----

  private final class EncryptingSerializerModifier extends BeanSerializerModifier {

    @Override
    public List<BeanPropertyWriter> changeProperties(
        SerializationConfig config,
        BeanDescription beanDesc,
        List<BeanPropertyWriter> beanProperties) {

      Map<String, String> encryptedFields = findEncryptedFields(beanDesc.getBeanClass());
      if (encryptedFields.isEmpty()) return beanProperties;

      List<BeanPropertyWriter> result = new ArrayList<>(beanProperties.size());
      Set<String> wrapped = new HashSet<>();
      Set<String> serializedComponents = new HashSet<>();
      for (BeanPropertyWriter writer : beanProperties) {
        // Resolve back to the RECORD COMPONENT name, not the external/JSON name. A
        // @JsonProperty-renamed @Encrypted component serializes under its JSON name
        // (writer.getName()), but findEncryptedFields keys by the component name — matching on the
        // JSON name would MISS the mapping and emit the PII field as PLAINTEXT (fail-open
        // PII leak). The writer's backing member (record accessor/field) carries the component
        // name.
        String componentName = resolveComponentName(writer);
        serializedComponents.add(componentName);
        String subjectIdField = encryptedFields.get(componentName);
        if (subjectIdField != null) {
          result.add(new EncryptingPropertyWriter(writer, subjectIdField));
          wrapped.add(componentName);
        } else {
          result.add(writer);
        }
      }
      // Fail CLOSED: every @Encrypted component MUST be wrapped in an encrypting writer. If any
      // could not be matched to a serialization property, refuse to build the serializer rather
      // than silently writing that PII field as plaintext at rest.
      if (!wrapped.containsAll(encryptedFields.keySet())) {
        Set<String> unresolved = new HashSet<>(encryptedFields.keySet());
        unresolved.removeAll(wrapped);
        throw new CryptoMappingException(
            "@Encrypted component(s) "
                + unresolved
                + " on "
                + beanDesc.getBeanClass().getName()
                + " could not be matched to a serialization property, so their values cannot be"
                + " encrypted. Refusing to serialize to avoid writing PII as PLAINTEXT at rest."
                + " Ensure each @Encrypted record component is serialized as a normal property"
                + " (e.g. it is not @JsonIgnore'd or hidden by a custom naming/introspection setup).");
      }
      // Fail CLOSED: every subjectId component referenced by an @Encrypted field must
      // ITSELF be serialized as a JSON property. The encrypting writer reads the subjectId
      // reflectively off the LIVE record, so with e.g. @JsonIgnore on the subjectId the write
      // would SUCCEED — ciphertext with no subjectId property in the stored JSON — and every
      // subsequent read would hit the null-subjectId read tolerance and silently return the
      // Base64 ciphertext AS the field value: permanently undecryptable data (the key reference
      // was never persisted) with zero error signal, and a confused GDPR-erasure story on top.
      // A @JsonProperty RENAME is fine — the property still serializes (under its external name)
      // and resolveComponentName keys it back to the component name checked here, while the read
      // side resolves the same rename through its component->JSON-name map.
      List<String> undecryptable = new ArrayList<>();
      for (String encComponent : new TreeSet<>(encryptedFields.keySet())) {
        String subjectIdComponent = encryptedFields.get(encComponent);
        if (!serializedComponents.contains(subjectIdComponent)) {
          undecryptable.add("'" + encComponent + "' (subjectId '" + subjectIdComponent + "')");
        }
      }
      if (!undecryptable.isEmpty()) {
        throw new CryptoMappingException(
            "@Encrypted component(s) "
                + undecryptable
                + " on "
                + beanDesc.getBeanClass().getName()
                + " reference subjectId component(s) that are NOT serialized as a JSON property"
                + " (e.g. @JsonIgnore'd). The ciphertext would be written WITHOUT its subjectId,"
                + " so no later read could ever derive the decryption key — permanently"
                + " undecryptable data — while the null-subjectId read tolerance would"
                + " silently hand back the Base64 ciphertext as the field value. Refusing to build"
                + " the serializer. Remove @JsonIgnore from the subjectId component (use a"
                + " @JsonProperty rename if its Java name must not appear on the wire), or"
                + " reference a different, serialized component as the subjectId.");
      }
      return result;
    }

    /**
     * Resolves a writer back to its RECORD COMPONENT name. For a record property the backing member
     * (accessor method or field) shares the component name even when {@link
     * com.fasterxml.jackson.annotation.JsonProperty @JsonProperty} renames the external JSON
     * property, so the member name is the reliable key into {@link #findEncryptedFields}. Falls
     * back to the writer's (external) name only when no member is available.
     */
    private String resolveComponentName(BeanPropertyWriter writer) {
      var member = writer.getMember();
      if (member != null && member.getName() != null) {
        return member.getName();
      }
      return writer.getName();
    }

    /**
     * Fail CLOSED when an {@code @Encrypted}-carrying type is about to be serialized by anything
     * other than a property-based bean serializer. {@code @JsonValue} makes Jackson build a {@code
     * JsonValueSerializer} instead of a {@code BeanSerializer}, so {@link #changeProperties} —
     * including its "every @Encrypted component must be wrapped" fail-closed check — never runs,
     * and the raw PII would be written verbatim into the append-only event store (empirically:
     * {@code "c1|pii@example.com"} with zero errors). This hook IS invoked for every serializer the
     * factory builds, with the type's own {@link BeanDescription}, so it is the one place a
     * non-bean serializer for an {@code @Encrypted}-carrying class can be caught.
     *
     * <p>Fail-closed is the only sound treatment (not faithful support): a {@code @JsonValue} wire
     * form is a single opaque scalar — there is no per-property slot to carry ciphertext plus
     * subjectId, so no encrypted round-trip can exist. The check is scoped to the described class's
     * OWN {@code @Encrypted} components: containers that merely CONTAIN encrypted-carrying records
     * (e.g. {@code @JsonUnwrapped} parents) have none of their own and pass through, and a
     * legitimate record's {@code BeanSerializer} (a {@link BeanSerializerBase}) — whose properties
     * just went through {@link #changeProperties} — passes as well.
     */
    @Override
    public JsonSerializer<?> modifySerializer(
        SerializationConfig config, BeanDescription beanDesc, JsonSerializer<?> serializer) {
      Map<String, String> encryptedFields = findEncryptedFields(beanDesc.getBeanClass());
      if (encryptedFields.isEmpty()) {
        return serializer;
      }
      if (!(serializer instanceof BeanSerializerBase beanSerializer)) {
        throw new CryptoMappingException(
            "@Encrypted component(s) "
                + new TreeSet<>(encryptedFields.keySet())
                + " on "
                + beanDesc.getBeanClass().getName()
                + " would be serialized by "
                + serializer.getClass().getName()
                + " instead of a property-based bean serializer, so the encrypting property"
                + " writers never run and the PII would be written as PLAINTEXT at rest. This"
                + " happens when the type's wire form is not a JSON object with per-property slots"
                + " — e.g. @JsonValue collapses the record to a single opaque value that cannot"
                + " carry ciphertext + subjectId, so no encrypted round-trip can exist. Remove"
                + " @JsonValue (or the custom serializer) from the @Encrypted-carrying record, or"
                + " drop @Encrypted and encrypt at the container level.");
      }
      // Class-level
      // @JsonIgnoreProperties/@JsonIncludeProperties on the record itself — and per-class config
      // overrides — are applied by BeanSerializerFactory.filterBeanProperties AFTER
      // changeProperties ran, so the construction-time checks above saw the full property list and
      // passed while the BUILT serializer silently lost the encrypting writer (PII vanishes from
      // the stored event) or its subjectId sibling (ciphertext stored without its key reference —
      // permanently undecryptable). Re-verify the final writer set on the built serializer.
      requireEncryptedWritersSurvive(beanSerializer, beanDesc, encryptedFields);
      // Wrap so per-property
      // @JsonIgnoreProperties/@JsonIncludeProperties on a REFERENCING property — applied later, in
      // BeanSerializerBase.createContextual via withByNameInclusion, where no modifier hook fires
      // — cannot silently drop those writers either. Only the exact stock BeanSerializer is
      // wrapped: the copy constructor is only guaranteed faithful for it, and every serializer
      // Jackson itself builds for a record lands here as one.
      if (serializer.getClass() == BeanSerializer.class) {
        return new FilterGuardedBeanSerializer(beanSerializer);
      }
      return serializer;
    }

    /**
     * Every {@code @Encrypted} component must survive to the BUILT serializer as an {@link
     * EncryptingPropertyWriter}, and every referenced subjectId component must survive as a writer,
     * whatever filtered the list between {@code changeProperties} and {@code build()} (class-level
     * {@code @JsonIgnoreProperties}/{@code @JsonIncludeProperties}, per-class config overrides,
     * type-id overlap removal, or a third-party modifier). Matching is by MEMBER (record-component)
     * name, so {@code @JsonProperty} renames and naming strategies never false-positive.
     */
    private void requireEncryptedWritersSurvive(
        BeanSerializerBase serializer,
        BeanDescription beanDesc,
        Map<String, String> encryptedFields) {
      Map<String, BeanPropertyWriter> writersByComponent = new HashMap<>();
      for (var it = serializer.properties(); it.hasNext(); ) {
        if (it.next() instanceof BeanPropertyWriter writer) {
          writersByComponent.put(resolveComponentName(writer), writer);
        }
      }
      Set<String> missing = new TreeSet<>();
      for (String encComponent : new TreeSet<>(encryptedFields.keySet())) {
        String subjectIdComponent = encryptedFields.get(encComponent);
        if (!(writersByComponent.get(encComponent) instanceof EncryptingPropertyWriter)) {
          missing.add(
              "'"
                  + encComponent
                  + "' (@Encrypted — the PII would silently VANISH from"
                  + " the stored event)");
        }
        if (!writersByComponent.containsKey(subjectIdComponent)) {
          missing.add(
              "'"
                  + subjectIdComponent
                  + "' (subjectId of '"
                  + encComponent
                  + "' — the"
                  + " ciphertext would be stored WITHOUT its key reference: permanently"
                  + " undecryptable)");
        }
      }
      if (!missing.isEmpty()) {
        throw new CryptoMappingException(
            "property filtering removed "
                + missing
                + " from the serializer built for "
                + beanDesc.getBeanClass().getName()
                + " AFTER the construction-time checks ran. This happens with a class-level"
                + " @JsonIgnoreProperties/@JsonIncludeProperties on the record (or a per-class"
                + " config override) naming an @Encrypted component or its subjectId. Refusing to"
                + " serialize. Remove the name from the class-level filter (use a @JsonProperty"
                + " rename if its Java name must not appear on the wire).");
      }
    }

    /**
     * Fail CLOSED when an {@code @Encrypted}-carrying type is used as a Map KEY. JSON object keys
     * serialize through a key serializer ({@code StdKeySerializers}), which stringifies the record
     * via {@code toString()} — a full plaintext dump of every component, PII included — and never
     * touches {@link #changeProperties}. A key is a single opaque string with no slot for
     * ciphertext + subjectId, so no encrypted round-trip can exist.
     */
    @Override
    public JsonSerializer<?> modifyKeySerializer(
        SerializationConfig config,
        JavaType valueType,
        BeanDescription beanDesc,
        JsonSerializer<?> serializer) {
      Map<String, String> encryptedFields = findEncryptedFields(valueType.getRawClass());
      if (encryptedFields.isEmpty()) {
        return serializer;
      }
      throw new CryptoMappingException(
          valueType.getRawClass().getName()
              + " carries @Encrypted component(s) "
              + new TreeSet<>(encryptedFields.keySet())
              + " and is used as a Map KEY: JSON object keys are serialized via toString(), which"
              + " would dump every component — including the PII — as PLAINTEXT, and a key is a"
              + " single opaque string with no property slot for ciphertext + subjectId, so no"
              + " encrypted round-trip can exist. Use the record as a Map VALUE, or key the map by"
              + " a non-PII identifier.");
    }
  }

  /**
   * A {@code BeanSerializer} that refuses by-name filtering of its encrypting writers and their
   * subjectId siblings.
   *
   * <p>{@code @JsonIgnoreProperties}/{@code @JsonIncludeProperties} on a REFERENCING property (the
   * container's member — directly, via mixin, on a {@code List}/{@code Map}/array/{@code
   * AtomicReference} member whose CONTENT is this record, or even a declared-{@code Object} member
   * resolved dynamically at write time) are applied when this record's serializer is contextualized
   * for that property: {@code BeanSerializerBase.createContextual} reads the member's ignorals and
   * filters the writer arrays via {@link #withByNameInclusion} — after every construction-time
   * check passed, with no modifier hook firing. Dropping the subjectId writer stores ciphertext
   * without its key reference (permanently undecryptable, then silently passed through as the field
   * value by the null-subjectId read tolerance); dropping the encrypting writer silently loses the
   * PII field from the append-only store. Both must fail loudly instead.
   *
   * <p>The guard lives on the record's OWN serializer rather than in an annotation-introspector
   * hook because only here are the effective EXTERNAL names authoritative: ignoral sets carry wire
   * names ({@code @JsonProperty} renames, naming strategies), and the filtering reaches this
   * serializer wherever the record is referenced — including through declared supertypes the
   * member's static type never reveals. Every fluent copy re-wraps, so a later contextualization of
   * a copy keeps the guard; filtering that names no protected writer proceeds untouched.
   *
   * <p>The guard extends to the two omission routes that are decided <em>per serialization</em>
   * rather than per contextualization — an active {@code @JsonView} and a {@code @JsonFilter} — see
   * {@link ProtectedWriterOmissionGuard}.
   */
  private static final class FilterGuardedBeanSerializer extends BeanSerializer {

    FilterGuardedBeanSerializer(BeanSerializerBase src) {
      super(src);
    }

    @Override
    protected BeanSerializerBase withByNameInclusion(Set<String> toIgnore, Set<String> toInclude) {
      ProtectedWriterOmissionGuard.refuseByNameFiltering(this, _props, toIgnore, toInclude);
      return new FilterGuardedBeanSerializer(super.withByNameInclusion(toIgnore, toInclude));
    }

    /** Same guard for the direct filtering entry point (used by JsonValueSerializer since 2.16). */
    @Override
    public JsonSerializer<?> withIgnoredProperties(Set<String> toIgnore) {
      ProtectedWriterOmissionGuard.refuseByNameFiltering(this, _props, toIgnore, null);
      return new FilterGuardedBeanSerializer(
          (BeanSerializerBase) super.withIgnoredProperties(toIgnore));
    }

    // The remaining fluent copies must preserve this class, or a later contextualization of the
    // copy would filter through the stock implementation unguarded.

    @Override
    public BeanSerializerBase withObjectIdWriter(ObjectIdWriter objectIdWriter) {
      return new FilterGuardedBeanSerializer(super.withObjectIdWriter(objectIdWriter));
    }

    @Override
    public BeanSerializerBase withFilterId(Object filterId) {
      return new FilterGuardedBeanSerializer(super.withFilterId(filterId));
    }

    @Override
    protected BeanSerializerBase withProperties(
        BeanPropertyWriter[] properties, BeanPropertyWriter[] filteredProperties) {
      return new FilterGuardedBeanSerializer(super.withProperties(properties, filteredProperties));
    }

    /**
     * {@code unwrappingSerializer} is the one fluent copy whose result is NOT a {@code
     * BeanSerializer}, so the stock override would hand back an unguarded {@link
     * UnwrappingBeanSerializer} — the only route by which an {@code @Encrypted}-carrying record's
     * serializer escapes this class. Verified reachable: an {@code @JsonUnwrapped} member of a
     * record whose subjectId carries a non-matching {@code @JsonView} writes {@code
     * {"c_email":"<ciphertext>"}} with no subjectId at all.
     */
    @Override
    public JsonSerializer<Object> unwrappingSerializer(NameTransformer unwrapper) {
      return new FilterGuardedUnwrappingBeanSerializer(this, unwrapper);
    }

    @Override
    protected void serializeFields(Object bean, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      ProtectedWriterOmissionGuard.refuseActiveViewOmission(this, _props, _filteredProps, provider);
      super.serializeFields(bean, gen, provider);
    }

    @Override
    protected void serializeFieldsFiltered(
        Object bean, JsonGenerator gen, SerializerProvider provider) throws IOException {
      ProtectedWriterOmissionGuard.refuseActiveViewOmission(this, _props, _filteredProps, provider);
      ProtectedWriterOmissionGuard.refusePropertyFilterOmission(
          this, _props, _filteredProps, _propertyFilterId, bean, provider);
      super.serializeFieldsFiltered(bean, gen, provider);
    }
  }

  /**
   * The {@code @JsonUnwrapped} flavour of {@link FilterGuardedBeanSerializer}. {@code
   * BeanSerializer.unwrappingSerializer} returns a stock {@link UnwrappingBeanSerializer}, which
   * runs the same {@code serializeFields}/{@code serializeFieldsFiltered} code paths off renamed
   * copies of the writer arrays — so without this subclass an unwrapped {@code @Encrypted}-carrying
   * record would lose every write-time guard. Each fluent copy re-wraps for the same reason the
   * non-unwrapping ones do.
   */
  private static final class FilterGuardedUnwrappingBeanSerializer
      extends UnwrappingBeanSerializer {

    FilterGuardedUnwrappingBeanSerializer(BeanSerializerBase src, NameTransformer transformer) {
      super(src, transformer);
    }

    private FilterGuardedUnwrappingBeanSerializer(
        UnwrappingBeanSerializer src, ObjectIdWriter objectIdWriter) {
      super(src, objectIdWriter);
    }

    private FilterGuardedUnwrappingBeanSerializer(
        UnwrappingBeanSerializer src, ObjectIdWriter objectIdWriter, Object filterId) {
      super(src, objectIdWriter, filterId);
    }

    private FilterGuardedUnwrappingBeanSerializer(
        UnwrappingBeanSerializer src, Set<String> toIgnore, Set<String> toInclude) {
      super(src, toIgnore, toInclude);
    }

    private FilterGuardedUnwrappingBeanSerializer(
        UnwrappingBeanSerializer src,
        BeanPropertyWriter[] properties,
        BeanPropertyWriter[] filteredProperties) {
      super(src, properties, filteredProperties);
    }

    @Override
    public JsonSerializer<Object> unwrappingSerializer(NameTransformer transformer) {
      // Same shape as the stock override (verified against the 2.19.2 bytecode:
      // new UnwrappingBeanSerializer(this, transformer)) — only the class differs.
      return new FilterGuardedUnwrappingBeanSerializer(this, transformer);
    }

    @Override
    public BeanSerializerBase withObjectIdWriter(ObjectIdWriter objectIdWriter) {
      return new FilterGuardedUnwrappingBeanSerializer(this, objectIdWriter);
    }

    @Override
    public BeanSerializerBase withFilterId(Object filterId) {
      return new FilterGuardedUnwrappingBeanSerializer(this, _objectIdWriter, filterId);
    }

    @Override
    protected BeanSerializerBase withByNameInclusion(Set<String> toIgnore, Set<String> toInclude) {
      ProtectedWriterOmissionGuard.refuseByNameFiltering(this, _props, toIgnore, toInclude);
      return new FilterGuardedUnwrappingBeanSerializer(this, toIgnore, toInclude);
    }

    @Override
    protected BeanSerializerBase withProperties(
        BeanPropertyWriter[] properties, BeanPropertyWriter[] filteredProperties) {
      return new FilterGuardedUnwrappingBeanSerializer(this, properties, filteredProperties);
    }

    @Override
    protected void serializeFields(Object bean, JsonGenerator gen, SerializerProvider provider)
        throws IOException {
      ProtectedWriterOmissionGuard.refuseActiveViewOmission(this, _props, _filteredProps, provider);
      super.serializeFields(bean, gen, provider);
    }

    @Override
    protected void serializeFieldsFiltered(
        Object bean, JsonGenerator gen, SerializerProvider provider) throws IOException {
      ProtectedWriterOmissionGuard.refuseActiveViewOmission(this, _props, _filteredProps, provider);
      ProtectedWriterOmissionGuard.refusePropertyFilterOmission(
          this, _props, _filteredProps, _propertyFilterId, bean, provider);
      super.serializeFieldsFiltered(bean, gen, provider);
    }
  }

  /**
   * The shared "an encrypting writer and its subjectId writer must both reach the wire" checks,
   * used by both guarded bean serializers.
   *
   * <p>Three routes can silently omit a protected writer <em>after</em> every construction-time
   * check has passed. They differ only in WHEN the omission is decided:
   *
   * <ul>
   *   <li><b>By-name</b> ({@code @JsonIgnoreProperties}/{@code @JsonIncludeProperties}) — decided
   *       at CONTEXTUALIZATION, so {@link #refuseByNameFiltering} runs from the fluent copies.
   *   <li><b>{@code @JsonView}</b> — decided per SERIALIZATION: {@code BeanSerializerFactory
   *       .processViews} builds a SEPARATE {@code _filteredProps} array (untouched by {@code
   *       modifySerializer}'s check over {@code serializer.properties()}, which reads only {@code
   *       _props}), and {@code serializeFields} switches to it only when the writer sets an active
   *       view. Whether a protected writer survives therefore depends on the view passed to {@code
   *       writerWithView(...)}, which no construction-time check can know — verified: the same
   *       record serializes correctly with a matching view and drops the subjectId with a
   *       non-matching one, and with {@code DEFAULT_VIEW_INCLUSION} disabled it drops the encrypted
   *       field itself. Hence {@link #refuseActiveViewOmission} runs at write time, where the
   *       active view is known, and stays silent when no view is active.
   *   <li><b>{@code @JsonFilter}</b> — decided per SERIALIZATION by a runtime-supplied {@code
   *       PropertyFilter} that can include or exclude any property by any rule. {@link
   *       #refusePropertyFilterOmission} asks that exact filter, for that exact bean, whether each
   *       protected writer would be included — so a filter that omits nothing protected keeps
   *       working (verified).
   * </ul>
   *
   * <p>All three fail closed with the same consequence framing: dropping the subjectId writer
   * stores ciphertext with no key reference (permanently undecryptable, then silently handed back
   * as the field value by the null-subjectId read tolerance); dropping the encrypting writer
   * silently loses the PII field from the append-only store.
   */
  private static final class ProtectedWriterOmissionGuard {

    private ProtectedWriterOmissionGuard() {}

    /**
     * Refuses ignore/include sets that would drop an encrypting writer or a subjectId writer, using
     * Jackson's own {@link IgnorePropertiesUtil#shouldIgnore} semantics against the serializer's
     * writers' external names.
     */
    static void refuseByNameFiltering(
        BeanSerializerBase serializer,
        BeanPropertyWriter[] props,
        Set<String> toIgnore,
        Set<String> toInclude) {
      Set<String> dropped = new TreeSet<>();
      for (BeanPropertyWriter writer : props) {
        if (!(writer instanceof EncryptingPropertyWriter encryptingWriter)) {
          continue;
        }
        if (IgnorePropertiesUtil.shouldIgnore(encryptingWriter.getName(), toIgnore, toInclude)) {
          dropped.add(describeEncryptedLoss(encryptingWriter.getName()));
        }
        String subjectIdExternalName =
            externalNameOfComponent(props, encryptingWriter.subjectIdField);
        if (subjectIdExternalName == null) {
          // Unreachable while the wrap-time presence check and this guard hold, but never let a
          // broken invariant pass silently.
          dropped.add(
              "'" + encryptingWriter.subjectIdField + "' (subjectId writer already absent)");
        } else if (IgnorePropertiesUtil.shouldIgnore(subjectIdExternalName, toIgnore, toInclude)) {
          dropped.add(describeSubjectIdLoss(subjectIdExternalName, encryptingWriter.getName()));
        }
      }
      if (!dropped.isEmpty()) {
        throw new CryptoMappingException(
            "@JsonIgnoreProperties/@JsonIncludeProperties on a property referencing "
                + serializer.handledType().getName()
                + " would filter "
                + dropped
                + " out of the serialized form (ignored="
                + toIgnore
                + ", included="
                + toInclude
                + "). Refusing to serialize. Remove the @Encrypted component's or its subjectId's"
                + " external name from the referencing property's filter annotation (a"
                + " @JsonProperty rename on the component is the supported way to change what"
                + " appears on the wire).");
      }
    }

    /**
     * Refuses an ACTIVE view that would omit a protected writer. No-op when no view is active or
     * the type declares none, so a view-free write (every write the framework itself performs)
     * costs two field reads.
     */
    static void refuseActiveViewOmission(
        BeanSerializerBase serializer,
        BeanPropertyWriter[] props,
        BeanPropertyWriter[] filteredProps,
        SerializerProvider provider) {
      Class<?> activeView = provider.getActiveView();
      if (activeView == null || filteredProps == null) {
        return;
      }
      Set<String> dropped = new TreeSet<>();
      for (int i = 0; i < props.length; i++) {
        if (!(props[i] instanceof EncryptingPropertyWriter encryptingWriter)) {
          continue;
        }
        if (!survivesView(props, filteredProps, i, activeView)) {
          dropped.add(describeEncryptedLoss(encryptingWriter.getName()));
        }
        int subjectIdIndex = indexOfComponent(props, encryptingWriter.subjectIdField);
        if (subjectIdIndex < 0) {
          dropped.add(
              "'" + encryptingWriter.subjectIdField + "' (subjectId writer already absent)");
        } else if (!survivesView(props, filteredProps, subjectIdIndex, activeView)) {
          dropped.add(
              describeSubjectIdLoss(props[subjectIdIndex].getName(), encryptingWriter.getName()));
        }
      }
      if (!dropped.isEmpty()) {
        throw new CryptoMappingException(
            "@JsonView is active ("
                + activeView.getName()
                + ") and would omit "
                + dropped
                + " from the serialized form of "
                + serializer.handledType().getName()
                + ". Refusing to serialize. A view that hides an @Encrypted component or its"
                + " subjectId corrupts the stored event exactly like a by-name filter would; keep"
                + " both properties in every view this mapper writes with (or serialize this record"
                + " through a mapper that sets no active view).");
      }
    }

    /**
     * Whether the writer at {@code index} is emitted under {@code activeView}, replicating {@code
     * BeanSerializerFactory.processViews} + {@code FilteredBeanPropertyWriter} exactly: a {@code
     * null} slot in {@code _filteredProps} is an unconditional omission (what {@code
     * DEFAULT_VIEW_INCLUSION=false} produces for a writer with no {@code @JsonView}), a writer with
     * no declared views that DID survive into the array is included by default, and a writer with
     * declared views is included only when one of them is assignable from the active view.
     */
    private static boolean survivesView(
        BeanPropertyWriter[] props,
        BeanPropertyWriter[] filteredProps,
        int index,
        Class<?> activeView) {
      if (index >= filteredProps.length || filteredProps[index] == null) {
        return false;
      }
      Class<?>[] views = props[index].getViews();
      if (views == null || views.length == 0) {
        return true;
      }
      for (Class<?> view : views) {
        if (view.isAssignableFrom(activeView)) {
          return true;
        }
      }
      return false;
    }

    /**
     * Refuses a {@code PropertyFilter} that would omit a protected writer for THIS bean. The
     * filter's decision is not otherwise observable — for JSON an excluded property produces no
     * callback at all ({@code SimpleBeanPropertyFilter} only calls {@code serializeAsOmittedField}
     * when {@code gen.canOmitFields()} is false) — so the filter is asked directly with a probe
     * writer that answers name/type/annotations exactly like the real one (it is a copy of it) but
     * writes nothing. The probe runs against a throwaway {@link TokenBuffer} rather than the live
     * generator, so even a filter that writes to the generator itself cannot corrupt the real
     * output.
     */
    static void refusePropertyFilterOmission(
        BeanSerializerBase serializer,
        BeanPropertyWriter[] props,
        BeanPropertyWriter[] filteredProps,
        Object propertyFilterId,
        Object bean,
        SerializerProvider provider)
        throws IOException {
      if (propertyFilterId == null) {
        return;
      }
      boolean viewFiltered = filteredProps != null && provider.getActiveView() != null;
      // Resolve exactly as StdSerializer.findPropertyFilter would, but WITHOUT its
      // reportBadDefinition branch: when no FilterProvider is configured at all, defer silently so
      // super still raises Jackson's own canonical "Cannot resolve PropertyFilter" error.
      FilterProvider filters = provider.getFilterProvider();
      PropertyFilter filter =
          filters == null ? null : filters.findPropertyFilter(propertyFilterId, bean);
      if (filter == null) {
        // No filter registered: BeanSerializerBase falls back to plain serializeFields, which the
        // view guard above already covered.
        return;
      }
      Set<String> dropped = new TreeSet<>();
      try (TokenBuffer probeOutput = new TokenBuffer(null, false)) {
        probeOutput.writeStartObject();
        for (int i = 0; i < props.length; i++) {
          if (!(props[i] instanceof EncryptingPropertyWriter encryptingWriter)) {
            continue;
          }
          if (!includedByFilter(
              props, filteredProps, viewFiltered, i, filter, bean, provider, probeOutput)) {
            dropped.add(describeEncryptedLoss(encryptingWriter.getName()));
          }
          int subjectIdIndex = indexOfComponent(props, encryptingWriter.subjectIdField);
          if (subjectIdIndex >= 0
              && !includedByFilter(
                  props,
                  filteredProps,
                  viewFiltered,
                  subjectIdIndex,
                  filter,
                  bean,
                  provider,
                  probeOutput)) {
            dropped.add(
                describeSubjectIdLoss(props[subjectIdIndex].getName(), encryptingWriter.getName()));
          }
        }
      }
      if (!dropped.isEmpty()) {
        throw new CryptoMappingException(
            "the @JsonFilter '"
                + propertyFilterId
                + "' registered for "
                + serializer.handledType().getName()
                + " would omit "
                + dropped
                + " from the serialized form. Refusing to serialize. A filter that hides an"
                + " @Encrypted component or its subjectId corrupts the stored event exactly like a"
                + " by-name filter would; the filter must include both properties for every bean"
                + " this mapper writes.");
      }
    }

    /**
     * Asks {@code filter} whether the writer at {@code index} would be written for {@code bean}.
     */
    private static boolean includedByFilter(
        BeanPropertyWriter[] props,
        BeanPropertyWriter[] filteredProps,
        boolean viewFiltered,
        int index,
        PropertyFilter filter,
        Object bean,
        SerializerProvider provider,
        TokenBuffer probeOutput)
        throws IOException {
      // Mirror BeanSerializerBase.serializeFieldsFiltered's array choice: under an active view the
      // filter is asked about the VIEW-filtered writer (a null slot is already an omission, which
      // refuseActiveViewOmission reports with a better message — don't double-report it here).
      BeanPropertyWriter effective = viewFiltered ? filteredProps[index] : props[index];
      if (effective == null) {
        return true;
      }
      InclusionProbeWriter probe = new InclusionProbeWriter(effective);
      try {
        filter.serializeAsField(bean, probeOutput, provider, probe);
      } catch (IOException | RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new CryptoMappingException(
            "the @JsonFilter for an @Encrypted-carrying type could not be evaluated", e);
      }
      return probe.included;
    }

    /** Resolves a record component name to its writer's external (wire) name, or null if gone. */
    private static String externalNameOfComponent(
        BeanPropertyWriter[] props, String componentName) {
      int index = indexOfComponent(props, componentName);
      return index < 0 ? null : props[index].getName();
    }

    /** Index of the writer backed by record component {@code componentName}, or {@code -1}. */
    private static int indexOfComponent(BeanPropertyWriter[] props, String componentName) {
      for (int i = 0; i < props.length; i++) {
        if (props[i] == null) {
          continue;
        }
        var member = props[i].getMember();
        if (member != null && componentName.equals(member.getName())) {
          return i;
        }
      }
      return -1;
    }

    private static String describeEncryptedLoss(String externalName) {
      return "'"
          + externalName
          + "' (@Encrypted — the PII would silently VANISH from the stored event)";
    }

    private static String describeSubjectIdLoss(String externalName, String encryptedName) {
      return "'"
          + externalName
          + "' (subjectId of '"
          + encryptedName
          + "' — the ciphertext would be stored WITHOUT its key reference: permanently"
          + " undecryptable)";
    }
  }

  /**
   * A faithful stand-in for a {@link BeanPropertyWriter} that records whether a {@link
   * PropertyFilter} chose to write it. Built with {@code BeanPropertyWriter}'s copy constructor, so
   * it answers name, full name, type, member and annotations exactly like the writer it stands in
   * for — everything a filter can base its decision on — while producing no output.
   */
  private static final class InclusionProbeWriter extends BeanPropertyWriter {

    private boolean included;

    InclusionProbeWriter(BeanPropertyWriter base) {
      super(base);
    }

    @Override
    public void serializeAsField(Object bean, JsonGenerator gen, SerializerProvider prov) {
      included = true;
    }

    @Override
    public void serializeAsElement(Object bean, JsonGenerator gen, SerializerProvider prov) {
      included = true;
    }

    @Override
    public void serializeAsOmittedField(Object bean, JsonGenerator gen, SerializerProvider prov) {
      // Explicitly excluded by the filter (only reached for formats that cannot omit fields).
    }

    @Override
    public void serializeAsPlaceholder(Object bean, JsonGenerator gen, SerializerProvider prov) {
      // No output; inclusion is signalled only by serializeAsField/serializeAsElement.
    }
  }

  /**
   * Fail CLOSED on class-level serialization overrides that replace the property-based bean
   * serializer <em>before</em> any {@link BeanSerializerModifier} hook can observe the class.
   * Empirically (jackson-databind 2.19): a class-level {@code @JsonSerialize(using = ...)} is
   * returned directly from the serializer factory's annotation lookup — neither {@code
   * changeProperties} nor {@code modifySerializer} ever fires for the class;
   * {@code @JsonSerialize(converter = ...)} re-routes construction to the converter's OUTPUT type
   * (the only {@code modifySerializer} call is for that delegate type); and
   * {@code @JsonSerialize(as = ...)} redirects to a different type's serializer whose plain writers
   * read this record's accessors — {@code @Encrypted} ones included — as ordinary plaintext
   * getters. In every case user code would write the PII verbatim with zero framework involvement.
   *
   * <p>{@code AnnotationIntrospector.findSerializer} is consulted for every class at serializer
   * construction, before any of those routes engage, and this introspector is registered via {@code
   * insertAnnotationIntrospector} (primary in the pair), so throwing here fails the write at the
   * same construction-time point as the module's other fail-closed guards. For classes without a
   * class-level {@code @JsonSerialize}, or without {@code @Encrypted} components, it returns {@code
   * null} and changes nothing.
   *
   * <p>Scoped to {@link AnnotatedClass}: a <em>property-level</em> {@code @JsonSerialize} declared
   * on ANOTHER type's field of the record type replaces the record's serializer for that one
   * property, and is the write-side mirror of the read-side residual — but a far worse failure: the
   * encrypting writers never run, so the PII lands in the append-only store as PLAINTEXT that no
   * later {@code forget(subjectId)} can reach (no key was ever minted). Verified against
   * jackson-databind 2.19.2 for {@code using}, {@code contentUsing}, {@code converter} and {@code
   * contentConverter}. The route IS observable here (the {@link Annotated} is an {@code
   * AnnotatedMember}), and is deliberately left unguarded for the same reason as its read-side
   * sibling: the annotation cannot be told apart from its legitimate shapes without running user
   * code — a custom serializer that pre-processes and then delegates back to the mapper encrypts
   * correctly, and a {@code converter} whose OUTPUT type is the record itself (a normalizing
   * pass-through) also keeps encrypting correctly (verified) — while a false positive would stop
   * the application persisting events at all. Documented as a hard constraint in {@code
   * docs/guide/advanced/gdpr-erasure.md} instead.
   */
  private static final class EncryptedSerializationOverrideGuard extends NopAnnotationIntrospector {

    @Override
    public Object findSerializer(Annotated a) {
      if (!(a instanceof AnnotatedClass)) {
        return null;
      }
      JsonSerialize ann = a.getAnnotation(JsonSerialize.class);
      if (ann == null) {
        return null;
      }
      Map<String, String> encryptedFields = findEncryptedFields(a.getRawType());
      if (encryptedFields.isEmpty()) {
        return null;
      }
      List<String> overrides = new ArrayList<>();
      if (ann.using() != JsonSerializer.None.class) {
        overrides.add("using = " + ann.using().getName());
      }
      if (ann.converter() != Converter.None.class) {
        overrides.add("converter = " + ann.converter().getName());
      }
      if (ann.as() != Void.class) {
        overrides.add("as = " + ann.as().getName());
      }
      if (overrides.isEmpty()) {
        return null;
      }
      throw new CryptoMappingException(
          "class-level @JsonSerialize("
              + String.join(", ", overrides)
              + ") on "
              + a.getRawType().getName()
              + " replaces the property-based bean serializer before the encrypting property"
              + " writers can be installed (no BeanSerializerModifier hook ever fires on this"
              + " path), so its @Encrypted component(s) "
              + new TreeSet<>(encryptedFields.keySet())
              + " would be written as PLAINTEXT by the override. Remove the class-level"
              + " @JsonSerialize override from the @Encrypted-carrying record, or drop @Encrypted"
              + " and encrypt at the container level.");
    }
  }

  /**
   * Fail CLOSED on class-level deserialization overrides that replace the bean deserializer
   * <em>before</em> {@code DecryptingDeserializerModifier} can wrap it. Verified against
   * jackson-databind 2.19.2 ({@code DeserializerCache._createDeserializer}):
   * {@code @JsonDeserialize(using = ...)} is returned straight from the annotation lookup; {@code
   * builder = ...} routes through the builder-based factory path, whose modifier hook receives the
   * BUILDER's {@code BeanDescription} (no {@code @Encrypted} components); {@code converter = ...}
   * builds a delegating deserializer for the converter's INPUT type. In every case the record is
   * silently constructed holding raw Base64 ciphertext — flowing into aggregates, read models and
   * APIs on every event replay, aggregate load, projection rebuild and DLQ replay; a GDPR forget
   * never surfaces {@code [REDACTED]} for the type (erasure becomes unverifiable), the redaction
   * observability never fires, and a snapshot re-serialization re-encrypts the ciphertext,
   * compounding the corruption one nesting level per write/read cycle.
   *
   * <p>The cache consults the introspector pair for {@code findDeserializer} / {@code
   * findPOJOBuilder} / {@code findDeserializationConverter} (primary — i.e. inserted — first)
   * before any of those routes engage, so throwing here fails the FIRST read at
   * deserializer-construction time, matching the write-side guards' fail-closed point. Each hook
   * refuses on the full annotation, so the guard holds even if Jackson reorders which one it asks
   * first. Writes are deliberately not failed: the serialize side is unaffected by the annotation
   * and stores correct ciphertext + subjectId, so data written while the override was present is
   * fully recoverable once it is removed.
   *
   * <p>Scoped to {@link AnnotatedClass}: property-level {@code @JsonDeserialize} on members —
   * whether on components OF the record (supported; the record's own decrypting deserializer still
   * wraps) or on ANOTHER type's field of the record type (the documented residual blind spot,
   * observable at {@code findDeserializer(AnnotatedMember)} but deliberately unguarded: a
   * per-property deserializer may legitimately pre-process and then delegate back to {@code
   * ctxt.readValue(...)}, which decrypts correctly, and a false positive on a read is a replay DoS)
   * — never triggers it.
   */
  private static final class EncryptedDeserializationOverrideGuard
      extends NopAnnotationIntrospector {

    @Override
    public Object findDeserializer(Annotated a) {
      failOnClassLevelOverrideOfEncryptedRecord(a);
      return null;
    }

    @Override
    public Object findDeserializationConverter(Annotated a) {
      failOnClassLevelOverrideOfEncryptedRecord(a);
      return null;
    }

    @Override
    public Class<?> findPOJOBuilder(AnnotatedClass ac) {
      failOnClassLevelOverrideOfEncryptedRecord(ac);
      return null;
    }

    private static void failOnClassLevelOverrideOfEncryptedRecord(Annotated a) {
      if (!(a instanceof AnnotatedClass)) {
        return;
      }
      JsonDeserialize ann = a.getAnnotation(JsonDeserialize.class);
      if (ann == null) {
        return;
      }
      Map<String, String> encryptedFields = findEncryptedFields(a.getRawType());
      if (encryptedFields.isEmpty()) {
        return;
      }
      List<String> overrides = new ArrayList<>();
      if (ann.using() != JsonDeserializer.None.class) {
        overrides.add("using = " + ann.using().getName());
      }
      if (ann.builder() != Void.class) {
        overrides.add("builder = " + ann.builder().getName());
      }
      if (ann.converter() != Converter.None.class) {
        overrides.add("converter = " + ann.converter().getName());
      }
      if (overrides.isEmpty()) {
        return;
      }
      throw new CryptoMappingException(
          "class-level @JsonDeserialize("
              + String.join(", ", overrides)
              + ") on "
              + a.getRawType().getName()
              + " replaces the bean deserializer before the decrypting deserializer can wrap it"
              + " (DeserializerCache returns the override ahead of every"
              + " BeanDeserializerModifier hook), so its @Encrypted component(s) "
              + new TreeSet<>(encryptedFields.keySet())
              + " would be read as raw Base64 ciphertext into the constructed record on every"
              + " replay/load/rebuild — and a GDPR forget would never surface [REDACTED] for this"
              + " type. Refusing to build the deserializer. Remove the class-level"
              + " @JsonDeserialize override from the @Encrypted-carrying record (data already"
              + " written by it is intact — ciphertext and subjectId were stored correctly), or"
              + " drop @Encrypted and encrypt at the container level.");
    }
  }

  private final class EncryptingPropertyWriter extends BeanPropertyWriter {

    private final BeanPropertyWriter delegate;
    private final String subjectIdField;

    EncryptingPropertyWriter(BeanPropertyWriter delegate, String subjectIdField) {
      super(delegate);
      this.delegate = delegate;
      this.subjectIdField = subjectIdField;
    }

    /**
     * Rename copy: same encryption binding, new external name. The {@code delegate} is only used
     * for member-based value access ({@link BeanPropertyWriter#get}), which a rename does not
     * affect; all name-bearing writes go through {@link #getName()} so the renamed copy emits the
     * transformed name.
     */
    private EncryptingPropertyWriter(EncryptingPropertyWriter base, PropertyName newName) {
      super(base, newName);
      this.delegate = base.delegate;
      this.subjectIdField = base.subjectIdField;
    }

    /**
     * Keep encrypting under a Jackson writer rename. When a type carrying {@code @Encrypted}
     * components is embedded via {@code @JsonUnwrapped(prefix/suffix)}, Jackson derives the
     * container's unwrapping serializer by calling {@link
     * BeanPropertyWriter#rename(NameTransformer)} on each of the inner type's writers; for a
     * CHANGED name that calls this factory, whose base implementation constructs a plain {@link
     * BeanPropertyWriter} copy — silently discarding this encrypting wrapper and appending the PII
     * as plaintext into the append-only event store (unreachable by any later GDPR forget). No
     * {@link BeanSerializerModifier} callback fires on that derivation path, so this override is
     * the only place the downgrade can be stopped. Mirrors Jackson's own {@code
     * UnwrappingBeanPropertyWriter}, which overrides the rename machinery to preserve itself.
     *
     * <p>The subjectId lookup is rename-proof by construction: {@link #getRecordComponentValue}
     * reads the live bean by RECORD COMPONENT name, which no external-name transform touches, and
     * {@link CryptoEngine#encrypt} derives nothing from the field name. The decrypt side resolves
     * the transformed names via {@code DecryptingDeserializer#unwrappingDeserializer}.
     */
    @Override
    protected BeanPropertyWriter _new(PropertyName newName) {
      return new EncryptingPropertyWriter(this, newName);
    }

    /**
     * Fail CLOSED on as-array shape. {@code @JsonFormat(shape = ARRAY)} routes serialization
     * through this method (via {@code BeanAsArraySerializer}), bypassing {@link #serializeAsField}
     * entirely — the base implementation would write the raw plaintext value as a positional array
     * element. There is no sound read side either: the decrypting deserializer resolves ciphertext
     * by property NAME in the stored JSON object, which a positional array does not carry, so an
     * "encrypted element" could never be reliably decrypted back. Refuse loudly instead of leaking
     * PII (or silently constructing records holding ciphertext).
     */
    @Override
    public void serializeAsElement(Object bean, JsonGenerator gen, SerializerProvider prov)
        throws Exception {
      throw new CryptoMappingException(
          "@JsonFormat(shape = ARRAY) is not supported on types carrying @Encrypted components"
              + " (field '"
              + getName()
              + "' on "
              + bean.getClass().getName()
              + "): positional array serialization bypasses the encrypting writer and would emit"
              + " the PII as PLAINTEXT, and a positional element carries no property name for the"
              + " decrypt side to resolve. Serialize the record as a JSON object instead.");
    }

    /**
     * Fail CLOSED if anything tries to derive an unwrapping writer from this one. Stock Jackson
     * only calls this during initial property construction (before the encrypting modifier runs),
     * so this is defense-in-depth against exotic/third-party callers: the returned {@code
     * UnwrappingBeanPropertyWriter} would be a base-class copy that drops encryption — never
     * acceptable for an {@code @Encrypted} (String) component, which has no meaningful unwrapped
     * form anyway.
     */
    @Override
    public BeanPropertyWriter unwrappingWriter(NameTransformer transformer) {
      throw new CryptoMappingException(
          "@JsonUnwrapped directly on @Encrypted component '"
              + getName()
              + "' is not supported: deriving an unwrapping writer would discard the encrypting"
              + " wrapper and emit the PII as PLAINTEXT. Remove @JsonUnwrapped from the @Encrypted"
              + " String component (unwrapping a scalar has no meaning).");
    }

    @Override
    public void serializeAsField(Object bean, JsonGenerator gen, SerializerProvider prov)
        throws Exception {
      Object value = delegate.get(bean);
      if (value == null) {
        // No PII present — serialize the null through this writer's resolved serializers.
        super.serializeAsField(bean, gen, prov);
        return;
      }

      Object subjectIdValue = getRecordComponentValue(bean, subjectIdField);
      if (subjectIdValue == null) {
        // Never fall back to plaintext: that would silently persist unencrypted PII.
        throw new CryptoMappingException(
            "Cannot encrypt field '"
                + getName()
                + "' on "
                + bean.getClass().getName()
                + ": subjectId field '"
                + subjectIdField
                + "' is null");
      }
      String subjectId = String.valueOf(subjectIdValue);
      String plaintext = String.valueOf(value);

      byte[] encrypted;
      try {
        encrypted =
            cryptoEngine.encrypt(
                SubjectId.of(subjectId), plaintext.getBytes(StandardCharsets.UTF_8));
      } catch (SubjectForgottenException e) {
        // Defect 6: re-serializing an already-forgotten aggregate's state (e.g. snapshotting
        // after a subject was crypto-shredded) must succeed instead of failing forever. Only do
        // this when the field already holds the [REDACTED] tombstone: that value is produced
        // exclusively by this module's own decrypt path on KeyNotFoundException, so seeing it here
        // together with the encrypt call's SubjectForgottenException proves the subject really is
        // forgotten (not merely a legitimate field whose plaintext happens to equal the marker
        // while the key still exists — that case never reaches this catch block, because encrypt
        // would have succeeded). Write the marker literally instead of propagating. A NEW real PII
        // value for a forgotten subject is never equal to REDACTED, so it never lands in this
        // catch block — the exception propagates and the write is still correctly rejected.
        if (REDACTED.equals(plaintext)) {
          gen.writeStringField(getName(), REDACTED);
          return;
        }
        throw e;
      }
      String base64 = Base64.getEncoder().encodeToString(encrypted);

      // getName(), not delegate.getName(): after a rename (@JsonUnwrapped prefix/suffix) THIS
      // writer carries the transformed external name while the delegate keeps the original one.
      gen.writeStringField(getName(), base64);
    }
  }

  // ---- Deserialization ----

  private final class DecryptingDeserializerModifier extends BeanDeserializerModifier {

    @Override
    public JsonDeserializer<?> modifyDeserializer(
        DeserializationConfig config, BeanDescription beanDesc, JsonDeserializer<?> deserializer) {

      Map<String, String> encryptedFields = findEncryptedFields(beanDesc.getBeanClass());
      if (encryptedFields.isEmpty()) return deserializer;

      // Map each record component name to its external JSON property name so a
      // @JsonProperty-renamed @Encrypted component (or its subjectId reference) can be located in
      // the stored JSON — decrypt happens on the buffered tokens by JSON name, keeping
      // rename parity on read.
      Map<String, String> componentToJson = new HashMap<>();
      for (BeanPropertyDefinition prop : beanDesc.findProperties()) {
        componentToJson.put(prop.getInternalName(), prop.getName());
      }
      return new DecryptingDeserializer(
          deserializer, beanDesc.getBeanClass(), encryptedFields, componentToJson);
    }
  }

  /**
   * A delegating deserializer that decrypts a record's {@link Encrypted} fields <em>before</em> the
   * record's canonical constructor runs. It buffers the incoming JSON object token for token,
   * replaces each {@code @Encrypted} field's Base64 ciphertext with its decrypted plaintext (or the
   * {@link #REDACTED} tombstone when the subject's key is gone), then replays the buffer through
   * the delegate to construct the record exactly once.
   *
   * <p>The buffer is lossless: every JSON number is kept as the {@code BigDecimal} of its literal
   * text (see {@link #bufferObject}), never narrowed to a double, so a {@code BigDecimal} component
   * keeps its scale and an {@code Instant}, {@code Duration} or {@code OffsetDateTime} written as
   * {@code seconds.nanos} keeps every nanosecond digit — the record reads back exactly as a record
   * without an {@code @Encrypted} component would, whatever the mapper's float handling. A JSON
   * tree would not do: Jackson builds a {@code DoubleNode} for every float unless the mapper
   * enables {@code USE_BIG_DECIMAL_FOR_FLOATS}, and even then its default node factory strips a
   * {@code BigDecimal}'s trailing zeros ({@code 19.90} becomes {@code 19.9}).
   *
   * <p>Because the compact constructor never sees ciphertext, a {@code @Encrypted} field may carry
   * ordinary format/parse validation (e.g. "email must contain '@'") that validates the real
   * plaintext on read — the pre-forget ciphertext-validation failure mode is eliminated. Only the
   * decrypted @Encrypted field values are swapped; every other token is replayed unchanged, so
   * nested and polymorphic records (whose own {@code @Encrypted} fields are handled by their own
   * per-type deserializer, and whose type discriminator is preserved in the buffer) behave exactly
   * as before. The one residual constraint is the genuinely unavoidable post-forget one: a
   * crypto-shredded field decrypts to {@link #REDACTED}, which a format check may still reject —
   * surfaced as an actionable {@link CryptoOperationException}.
   */
  private final class DecryptingDeserializer extends DelegatingDeserializer {

    private final Class<?> beanClass;
    private final Map<String, String> encryptedFields;
    private final Map<String, String> componentToJson;

    DecryptingDeserializer(
        JsonDeserializer<?> delegate,
        Class<?> beanClass,
        Map<String, String> encryptedFields,
        Map<String, String> componentToJson) {
      super(delegate);
      this.beanClass = beanClass;
      this.encryptedFields = encryptedFields;
      this.componentToJson = componentToJson;
    }

    @Override
    protected JsonDeserializer<?> newDelegatingInstance(JsonDeserializer<?> newDelegate) {
      return new DecryptingDeserializer(newDelegate, beanClass, encryptedFields, componentToJson);
    }

    /**
     * Keep decrypting under a Jackson name transform. When this record is embedded via
     * {@code @JsonUnwrapped(prefix/suffix)}, the container's deserializer derives an unwrapping
     * variant whose buffered JSON carries the TRANSFORMED property names (e.g. {@code cust_email}).
     * The inherited {@link DelegatingDeserializer} implementation re-wraps the derived delegate via
     * {@link #newDelegatingInstance} — preserving decryption but with the ORIGINAL
     * component-to-JSON-name map, so the property lookup would miss the prefixed ciphertext and the
     * record would silently be constructed holding Base64 ciphertext. Transform the map alongside
     * the names so ciphertext and subjectId are resolved under their external, prefixed names.
     * Composes under nested unwrapping: each derivation transforms the previous map.
     */
    @Override
    public JsonDeserializer<Object> unwrappingDeserializer(NameTransformer unwrapper) {
      JsonDeserializer<?> unwrapped = getDelegatee().unwrappingDeserializer(unwrapper);
      if (unwrapped == getDelegatee()) {
        // The delegate considered the transform a no-op, so property names are unchanged.
        return this;
      }
      Map<String, String> transformedComponentToJson = new HashMap<>();
      for (Map.Entry<String, String> entry : componentToJson.entrySet()) {
        transformedComponentToJson.put(entry.getKey(), unwrapper.transform(entry.getValue()));
      }
      return new DecryptingDeserializer(
          unwrapped, beanClass, encryptedFields, transformedComponentToJson);
    }

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      if (!beanClass.isRecord()) {
        return super.deserialize(p, ctxt);
      }
      JsonToken current = p.currentToken();
      if (current != JsonToken.START_OBJECT && current != JsonToken.FIELD_NAME) {
        // Nothing to decrypt: an explicit null, a non-object shape, or an object whose properties a
        // type-id reader already consumed (it hands over the closing brace). The delegate reads the
        // live parser exactly as it would without this module.
        return super.deserialize(p, ctxt);
      }
      try (TokenBuffer stored = bufferObject(p, ctxt)) {
        Map<String, ScalarProperty> scalars = topLevelScalars(stored);
        Map<String, String> plaintextByJsonName = new HashMap<>();
        List<String> forgottenComponents = new ArrayList<>();
        decryptEncryptedFields(scalars, plaintextByJsonName, forgottenComponents);
        if (plaintextByJsonName.isEmpty()) {
          return delegateFrom(stored, ctxt, forgottenComponents);
        }
        try (TokenBuffer decrypted = withPlaintext(stored, plaintextByJsonName, ctxt)) {
          return delegateFrom(decrypted, ctxt, forgottenComponents);
        }
      }
    }

    /**
     * Copies the record's stored JSON object into a {@link TokenBuffer}, token for token. Every
     * number is buffered as the {@code BigDecimal} of its literal text ({@link
     * TokenBuffer#forceUseOfBigDecimal}), never narrowed to a double, so the replay hands the
     * delegate the same numeric tokens a direct read would. The parser arrives on the opening brace
     * or — after a type-id reader consumed the opening brace and the type property — on the next
     * property name; it is left on the object's closing brace, where a direct read leaves it.
     */
    private static TokenBuffer bufferObject(JsonParser p, DeserializationContext ctxt)
        throws IOException {
      TokenBuffer stored = ctxt.bufferForInputBuffering(p);
      stored.forceUseOfBigDecimal(true);
      stored.writeStartObject();
      JsonToken t = p.currentToken() == JsonToken.START_OBJECT ? p.nextToken() : p.currentToken();
      while (t == JsonToken.FIELD_NAME) {
        stored.copyCurrentStructure(p); // the property name and its whole value
        t = p.nextToken();
      }
      stored.writeEndObject();
      return stored;
    }

    /** A top-level property holding a scalar value: the token it was stored as, and its text. */
    private record ScalarProperty(JsonToken token, String text) {}

    /**
     * The stored object's top-level properties that hold a non-null scalar value, by JSON name.
     * Nested objects and arrays are skipped: a ciphertext is always a string and a subjectId
     * component is always a String, so neither is ever nested.
     */
    private static Map<String, ScalarProperty> topLevelScalars(TokenBuffer stored)
        throws IOException {
      Map<String, ScalarProperty> scalars = new HashMap<>();
      try (JsonParser replay = stored.asParserOnFirstToken()) {
        while (replay.nextToken() == JsonToken.FIELD_NAME) {
          String name = replay.currentName();
          JsonToken value = replay.nextToken();
          if (value.isScalarValue() && value != JsonToken.VALUE_NULL) {
            scalars.put(name, new ScalarProperty(value, replay.getText()));
          }
          replay.skipChildren();
        }
      }
      return scalars;
    }

    /**
     * Decrypts each {@link Encrypted} field's stored ciphertext, collecting the plaintext to
     * substitute by JSON name. A null/absent value is left untouched; the {@link #REDACTED}
     * tombstone stored literally (a snapshot re-serialized after a forget) is passed through as-is
     * (never Base64-decoded); a null subjectId leaves the stored value untouched (an event written
     * before its type gained the subjectId component, or whose subjectId an upcaster dropped, names
     * no key to decrypt it with). Components that decrypt to {@link #REDACTED} (crypto-shredded)
     * are recorded so {@link #delegateFrom} can surface an actionable error if the constructor
     * rejects the tombstone.
     */
    private void decryptEncryptedFields(
        Map<String, ScalarProperty> scalars,
        Map<String, String> plaintextByJsonName,
        List<String> forgottenComponents) {
      for (Map.Entry<String, String> entry : encryptedFields.entrySet()) {
        String encComponent = entry.getKey();
        String subjectIdComponent = entry.getValue();
        String encJsonName = componentToJson.getOrDefault(encComponent, encComponent);
        String subjectJsonName =
            componentToJson.getOrDefault(subjectIdComponent, subjectIdComponent);

        ScalarProperty storedValue = scalars.get(encJsonName);
        if (storedValue == null || storedValue.token() != JsonToken.VALUE_STRING) {
          continue; // no ciphertext present (null PII / absent field)
        }
        String base64Value = storedValue.text();
        if (REDACTED.equals(base64Value)) {
          // Defect 6: a snapshot re-serialized after the subject was forgotten stores the
          // [REDACTED] tombstone literally (see EncryptingPropertyWriter). Pass it through as-is
          // instead of attempting to Base64-decode/decrypt it.
          continue;
        }
        ScalarProperty storedSubjectId = scalars.get(subjectJsonName);
        if (storedSubjectId == null) {
          continue; // cannot decrypt without a subjectId — keep the stored value as-is
        }
        String subjectId = storedSubjectId.text();
        String decrypted = decryptValue(subjectId, base64Value);
        plaintextByJsonName.put(encJsonName, decrypted);
        if (REDACTED.equals(decrypted)) {
          // Subject was crypto-shredded: this field now holds the tombstone, which the record's
          // constructor may reject (see delegateFrom's actionable error). The raw
          // subjectId read off the stored JSON may itself be PII, and this string is embedded into
          // a
          // CryptoOperationException message that reaches logs / DLQ error_message — so record the
          // SHA-256 hash, never the raw id (matching this module's own subject-hash logging).
          forgottenComponents.add(encComponent + " (subject-hash=" + subjectHash(subjectId) + ")");
        }
      }
    }

    /**
     * Copies the stored object into a new buffer, writing each decrypted plaintext in place of its
     * ciphertext string. Every other token — every number still a {@code BigDecimal} — is copied
     * unchanged, so the delegate constructs the record from exactly what was stored, with only the
     * {@code @Encrypted} values swapped for plaintext.
     */
    private static TokenBuffer withPlaintext(
        TokenBuffer stored, Map<String, String> plaintextByJsonName, DeserializationContext ctxt)
        throws IOException {
      try (JsonParser replay = stored.asParserOnFirstToken()) {
        TokenBuffer decrypted = ctxt.bufferForInputBuffering(replay);
        decrypted.forceUseOfBigDecimal(true);
        decrypted.writeStartObject();
        while (replay.nextToken() == JsonToken.FIELD_NAME) {
          String name = replay.currentName();
          decrypted.writeFieldName(name);
          JsonToken value = replay.nextToken();
          String plaintext = value == JsonToken.VALUE_STRING ? plaintextByJsonName.get(name) : null;
          if (plaintext != null) {
            decrypted.writeString(plaintext);
          } else {
            decrypted.copyCurrentStructure(replay); // the whole value, scalar or nested
          }
        }
        decrypted.writeEndObject();
        return decrypted;
      }
    }

    /**
     * Replays the buffered (decrypted) object through the delegate so the record is constructed
     * exactly once, from plaintext. If construction fails and one or more {@code @Encrypted} fields
     * decrypted to the post-forget {@link #REDACTED} tombstone, surfaces the actionable error
     * naming the record, field(s), and forgotten subject(s) instead of the opaque constructor
     * failure.
     */
    private Object delegateFrom(
        TokenBuffer tokens, DeserializationContext ctxt, List<String> forgottenComponents)
        throws IOException {
      try (JsonParser replay = tokens.asParserOnFirstToken()) {
        return super.deserialize(replay, ctxt);
      } catch (IOException | RuntimeException e) {
        if (!forgottenComponents.isEmpty()) {
          // After a GDPR forget, a crypto-shredded subject's @Encrypted field decrypts to
          // the "[REDACTED]" tombstone. If the record's canonical/compact constructor rejects the
          // sentinel, the aggregate would become permanently unloadable — erasure turns into a DoS
          // on that stream. A record CANNOT be constructed while bypassing its compact-constructor
          // validation, so the field MUST tolerate the sentinel. Surface a targeted, actionable
          // failure naming the record, the offending @Encrypted field(s), and the forgotten
          // subject(s) so the erasure-induced failure is immediately diagnosable and fixable.
          throw new CryptoMappingException(
              "Cannot reconstruct "
                  + beanClass.getName()
                  + " after crypto-shredding: its constructor rejected the \""
                  + REDACTED
                  + "\" tombstone substituted for forgotten @Encrypted field(s) "
                  + forgottenComponents
                  + ". A crypto-shredded (GDPR-erased) subject's @Encrypted fields decrypt to the \""
                  + REDACTED
                  + "\" sentinel, so the record's validation MUST accept that sentinel (e.g. skip"
                  + " format/length checks when the value equals CryptoShreddingModule.REDACTED),"
                  + " otherwise the aggregate becomes permanently unloadable after erasure. Relax the"
                  + " constructor validation for the encrypted field(s) to tolerate the tombstone.",
              e);
        }
        throw e;
      }
    }

    /**
     * Decrypts a single field value. Returns {@link #REDACTED} only when the subject's key is
     * definitively gone ({@link KeyNotFoundException}, i.e. crypto-shredded). Any other failure —
     * transient backend outage, corrupt Base64, bad ciphertext — propagates so event replay does
     * not silently turn recoverable data into tombstones.
     */
    private String decryptValue(String subjectId, String base64Value) {
      byte[] encrypted = Base64.getDecoder().decode(base64Value);
      try {
        byte[] decrypted = cryptoEngine.decrypt(SubjectId.of(subjectId), encrypted);
        onDecryptSuccess();
        return new String(decrypted, StandardCharsets.UTF_8);
      } catch (KeyNotFoundException _) {
        onSubjectRedacted(subjectId);
        return REDACTED;
      }
    }
  }

  /**
   * A successful decrypt proves the key store is reachable and serving keys, so it ends any run of
   * redactions the systemic-failure detector was accumulating and clears a raised alarm.
   *
   * <p>The guard checks the LATCH as well as the run set, not the run set alone. {@link
   * #onSubjectRedacted} bounds an ongoing wipe by clearing the run set once {@code distinct >
   * threshold * 2} while deliberately leaving the latch raised (one alarm per episode). With the
   * set-only guard, a recovery that landed after that bound-clear did nothing: the set was already
   * empty, so the latch was never lowered and {@code systemicAlarmRaised} stayed true for the whole
   * process lifetime. From that point a SECOND key-store wipe failed the {@code
   * compareAndSet(false, true)} and emitted neither {@code
   * streamrune.crypto.keystore_systemic_failure} (documented as "page on any nonzero value") nor
   * its ERROR, and every per-subject WARN stayed suppressed by the already-alarmed branch — leaving
   * only the raw {@code subject_redacted} counter, the very signal the systemic alarm exists to
   * disambiguate.
   *
   * <p>The alternative — clearing the latch inside that bound branch — was rejected: it would
   * re-raise the alarm every {@code 2 * threshold} distinct subjects <em>within a single
   * outage</em> (a projection rebuild against a wiped key store pages continuously), breaking the
   * "emitted once per detected outage episode" contract in the opposite direction. Only evidence
   * that the key store is serving keys again may re-arm it.
   */
  private void onDecryptSuccess() {
    if (systemicAlarmRaised.get() || !distinctRedactedSinceSuccess.isEmpty()) {
      distinctRedactedSinceSuccess.clear();
      systemicAlarmRaised.set(false);
    }
  }

  /**
   * Records a {@code KeyNotFoundException} -&gt; {@code [REDACTED]} fallback: always bumps the
   * {@code streamrune.crypto.subject_redacted} metric, and distinguishes a lawful per-subject
   * forget (rate-limited {@code WARN}) from a systemic key-store failure — a run of redactions for
   * many distinct subjects with no successful decrypt in between, which trips one {@code ERROR}
   * alarm. Subject ids are hashed before logging so PII never lands in logs.
   */
  private void onSubjectRedacted(String rawSubjectId) {
    metrics.recordSubjectRedacted();
    String subjectHash = subjectHash(rawSubjectId);
    boolean newInThisRun = distinctRedactedSinceSuccess.add(subjectHash);
    int distinct = distinctRedactedSinceSuccess.size();

    if (distinct >= systemicRedactionThreshold && systemicAlarmRaised.compareAndSet(false, true)) {
      metrics.recordSystemicKeyStoreFailure();
      log.error(
          "SYSTEMIC key-store failure suspected: {} DISTINCT subjects decrypted to \"{}\" with no"
              + " successful decrypt in between. This is far beyond normal GDPR-erasure volume and"
              + " indicates the crypto key store is unavailable or empty (a DB restored without the"
              + " key rows, a truncated key table, or the engine pointed at the wrong datasource) —"
              + " every @Encrypted field is being written into read models as \"{}\", silently"
              + " corrupting them and masquerading as lawful erasure. Verify the key store"
              + " immediately; do NOT treat these redactions as right-to-erasure.",
          distinct,
          REDACTED,
          REDACTED);
    }

    if (systemicAlarmRaised.get()) {
      // Already alarmed once for this outage: keep the metric flowing, but suppress per-subject
      // WARNs (the ERROR told the story) and bound the run set so an ongoing wipe cannot grow it
      // without limit.
      if (distinct > systemicRedactionThreshold * 2L) {
        distinctRedactedSinceSuccess.clear();
      }
      return;
    }

    if (newInThisRun) {
      log.warn(
          "decrypt fell back to \"{}\" for subject-hash {}: its key is absent — a legitimately"
              + " crypto-shredded (GDPR-erased) subject, OR a key the store is missing. Expected for"
              + " a lawfully erased subject; a rising streamrune.crypto.subject_redacted rate (or an"
              + " ERROR alarm) means the key store itself is failing, not that subjects were erased.",
          REDACTED,
          subjectHash);
    }
  }

  /** SHA-256 hex of a subject id — logged instead of the raw id, which may itself be PII. */
  private static String subjectHash(String subjectId) {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(subjectId.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (Exception _) {
      // Never let a hashing hiccup break decryption/observability — fall back to a non-PII marker.
      return "unavailable";
    }
  }
}
