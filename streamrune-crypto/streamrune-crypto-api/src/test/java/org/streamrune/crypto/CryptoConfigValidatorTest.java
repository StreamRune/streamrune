package org.streamrune.crypto;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.invoke.MethodHandles;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.test.UnregisteredSealedTypes;

/**
 * Verifies that {@link CryptoConfigValidator} fails fast at startup when a registered type (event,
 * state, or command) carries an {@link Encrypted} field but no {@code CryptoEngine} is configured —
 * the root cause of silently persisting plaintext PII (see {@code CryptoShreddingModule}, which
 * only intercepts {@code @Encrypted} fields when registered on the ObjectMapper).
 */
class CryptoConfigValidatorTest {

  record UserRegistered(String userId, @Encrypted(subjectId = "userId") String email) {}

  record OrderCreated(String orderId, String sku) {}

  // ── A sealed type whose permitted subclasses cannot be read (native image, unregistered) ──────
  //
  // A GraalVM native image reports a sealed type that is not registered for reflection as sealed
  // with NO permitted subclasses. permittedOrEmpty() used to read that as "no subtypes", so an
  // @Encrypted field of any subtype escaped the guard silently — the startup check passed and the
  // PII was persisted as plaintext. Every walk now refuses the type instead.

  @Test
  void validate_refusesASealedTypeWhosePermittedSubclassesCannotBeRead_withoutAnEngine() {
    Class<?> unregistered = UnregisteredSealedTypes.sealedInterface(MethodHandles.lookup());

    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> CryptoConfigValidator.validate(List.of(unregistered), null));

    assertTrue(ex.getMessage().contains(unregistered.getName()), ex.getMessage());
    assertTrue(ex.getMessage().contains("reports no permitted subclasses"), ex.getMessage());
  }

  @Test
  void validate_refusesASealedTypeWhosePermittedSubclassesCannotBeRead_withAnEngine() {
    // The property-override detector walks the hierarchy even when an engine is configured — the
    // case it exists for — so it cannot treat the unreadable type as a leaf either.
    Class<?> unregistered = UnregisteredSealedTypes.sealedInterface(MethodHandles.lookup());
    var engine = new InMemoryCryptoEngine();

    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> CryptoConfigValidator.validate(List.of(unregistered), engine));

    assertTrue(ex.getMessage().contains(unregistered.getName()), ex.getMessage());
  }

  @Test
  void referencesEncrypted_refusesASealedTypeWhosePermittedSubclassesCannotBeRead() {
    Class<?> unregistered = UnregisteredSealedTypes.sealedInterface(MethodHandles.lookup());

    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> CryptoConfigValidator.referencesEncrypted(unregistered));

    assertTrue(ex.getMessage().contains(unregistered.getName()), ex.getMessage());
  }

  @Test
  void throwsWhenEncryptedTypeHasNoCryptoEngine() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> CryptoConfigValidator.validate(List.of(UserRegistered.class), null));

    assertTrue(ex.getMessage().contains("UserRegistered"), ex.getMessage());
    assertTrue(ex.getMessage().contains("email"), ex.getMessage());
    assertTrue(ex.getMessage().contains("CryptoEngine"), ex.getMessage());
  }

  @Test
  void doesNotThrowWhenCryptoEngineIsConfigured() {
    assertDoesNotThrow(
        () ->
            CryptoConfigValidator.validate(
                List.of(UserRegistered.class), new InMemoryCryptoEngine()));
  }

  @Test
  void doesNotThrowWhenNoTypeHasEncryptedFields() {
    assertDoesNotThrow(() -> CryptoConfigValidator.validate(List.of(OrderCreated.class), null));
  }

  @Test
  void doesNotThrowForEmptyTypeCollection() {
    assertDoesNotThrow(() -> CryptoConfigValidator.validate(List.of(), null));
  }

  @Test
  void messageListsAllOffendingClassesAndFields() {
    record PaymentTaken(String paymentId, @Encrypted(subjectId = "paymentId") String cardNumber) {}

    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                CryptoConfigValidator.validate(
                    List.of(UserRegistered.class, PaymentTaken.class), null));

    assertTrue(ex.getMessage().contains("UserRegistered"), ex.getMessage());
    assertTrue(ex.getMessage().contains("email"), ex.getMessage());
    assertTrue(ex.getMessage().contains("PaymentTaken"), ex.getMessage());
    assertTrue(ex.getMessage().contains("cardNumber"), ex.getMessage());
  }

  @Test
  void ignoresNonRecordAndNonEncryptedTypesMixedWithOffenders() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                CryptoConfigValidator.validate(
                    List.of(OrderCreated.class, UserRegistered.class), null));

    assertFalse(ex.getMessage().contains("OrderCreated"), ex.getMessage());
    assertTrue(ex.getMessage().contains("UserRegistered"), ex.getMessage());
  }

  // Nested records: CryptoShreddingModule encrypts @Encrypted fields anywhere in the serialized
  // object graph (Jackson recurses into nested beans), so the startup guard must too.
  record Address(String customerId, @Encrypted(subjectId = "customerId") String street) {}

  record CustomerState(String id, Address address) {}

  record CustomerStateWithAddresses(String id, List<Address> addresses) {}

  @Test
  void throwsWhenNestedRecordHasEncryptedFieldAndNoEngine() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> CryptoConfigValidator.validate(List.of(CustomerState.class), null));
    assertTrue(ex.getMessage().contains("Address"), ex.getMessage());
    assertTrue(ex.getMessage().contains("street"), ex.getMessage());
  }

  @Test
  void throwsWhenEncryptedRecordNestedInCollectionAndNoEngine() {
    assertThrows(
        IllegalStateException.class,
        () -> CryptoConfigValidator.validate(List.of(CustomerStateWithAddresses.class), null));
  }

  @Test
  void nestedEncryptedDoesNotThrowWhenEngineConfigured() {
    assertDoesNotThrow(
        () ->
            CryptoConfigValidator.validate(
                List.of(CustomerState.class), new InMemoryCryptoEngine()));
  }

  // @Encrypted lives on a record component, but the path from a registered type may
  // cross a NON-record container or a (sealed) interface-typed component — Jackson (and thus
  // CryptoShreddingModule's per-bean serializer modifier) still descends into those, so the
  // validator must too, or the nested @Encrypted record's PII is written as silent plaintext.
  record CustomerInfo(String customerId, @Encrypted(subjectId = "customerId") String email) {}

  static final class OrderPlaced { // non-record registered event holding a record with @Encrypted
    CustomerInfo customer;
  }

  sealed interface PaymentMethod permits CardPayment, CashPayment {}

  record CardPayment(String paymentId, @Encrypted(subjectId = "paymentId") String pan)
      implements PaymentMethod {}

  record CashPayment(int amount) implements PaymentMethod {}

  record PaymentTakenEvent(String id, PaymentMethod method) {}

  static final class PlainHolder { // non-record with nothing @Encrypted reachable
    String name;
    int count;
    OrderCreated order;
  }

  @Test
  void throwsWhenEncryptedRecordNestedInNonRecordHolderAndNoEngine() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> CryptoConfigValidator.validate(List.of(OrderPlaced.class), null));
    assertTrue(ex.getMessage().contains("CustomerInfo"), ex.getMessage());
    assertTrue(ex.getMessage().contains("email"), ex.getMessage());
  }

  @Test
  void throwsWhenEncryptedRecordBehindSealedInterfaceAndNoEngine() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> CryptoConfigValidator.validate(List.of(PaymentTakenEvent.class), null));
    assertTrue(ex.getMessage().contains("CardPayment"), ex.getMessage());
    assertTrue(ex.getMessage().contains("pan"), ex.getMessage());
  }

  @Test
  void doesNotThrowForNonRecordHolderWithoutEncryptedField() {
    assertDoesNotThrow(() -> CryptoConfigValidator.validate(List.of(PlainHolder.class), null));
  }

  @Test
  void nonRecordHolderWithEncryptedDoesNotThrowWhenEngineConfigured() {
    assertDoesNotThrow(
        () ->
            CryptoConfigValidator.validate(List.of(OrderPlaced.class), new InMemoryCryptoEngine()));
  }

  // ── Property-level @JsonSerialize/@JsonDeserialize over an @Encrypted-bearing type ──
  //
  // Three tests in CryptoShreddingModuleTest ASSERT the leak — assertTrue(json.contains(
  // "pii@example.com")) — because a *serializer-time* guard cannot tell a bypass from a legitimate
  // delegating override. That argument is sound and does not preclude a *boot-time* detector, and
  // until now there was none: CryptoConfigValidator never looked at @JsonSerialize at all, so the
  // only control was prose in gdpr-erasure.md. These pin the detector.

  record CustomerWithPii(String customerId, @Encrypted(subjectId = "customerId") String email) {}

  /** A serializer that writes the nested record's fields itself — the bypass shape. */
  static final class PlaintextCustomerSerializer
      extends com.fasterxml.jackson.databind.JsonSerializer<CustomerWithPii> {
    @Override
    public void serialize(
        CustomerWithPii value,
        com.fasterxml.jackson.core.JsonGenerator gen,
        com.fasterxml.jackson.databind.SerializerProvider provider)
        throws java.io.IOException {
      gen.writeStartObject();
      gen.writeStringField("email", value.email());
      gen.writeEndObject();
    }
  }

  record OrderWithPropertySerializerUsing(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              using = PlaintextCustomerSerializer.class)
          CustomerWithPii customer) {}

  record OrderWithPropertySerializerContentUsing(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              contentUsing = PlaintextCustomerSerializer.class)
          List<CustomerWithPii> customers) {}

  /** Typing-only refinement: it does not replace the serializer, so it is not a bypass. */
  record OrderWithTypingOnlyAnnotation(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(as = Object.class)
          CustomerWithPii customer) {}

  /** No @Encrypted anywhere under the annotated component — must stay silent. */
  record PlainSku(String code) {}

  record OrderWithOverrideOnPlainComponent(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(using = PlainSkuSerializer.class)
          PlainSku sku) {}

  static final class PlainSkuSerializer
      extends com.fasterxml.jackson.databind.JsonSerializer<PlainSku> {
    @Override
    public void serialize(
        PlainSku value,
        com.fasterxml.jackson.core.JsonGenerator gen,
        com.fasterxml.jackson.databind.SerializerProvider provider)
        throws java.io.IOException {
      gen.writeString(value.code());
    }
  }

  @Test
  void warnsWhenPropertyLevelSerializerOverridesAnEncryptedBearingComponent() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithPropertySerializerUsing.class), offenders);

    assertEquals(1, offenders.size(), offenders.toString());
    assertTrue(offenders.get(0).contains("OrderWithPropertySerializerUsing"), offenders.toString());
    assertTrue(offenders.get(0).endsWith("#customer"), offenders.toString());
  }

  @Test
  void warnsForContentUsingOverAListOfEncryptedRecords() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithPropertySerializerContentUsing.class), offenders);

    assertEquals(1, offenders.size(), offenders.toString());
    assertTrue(offenders.get(0).endsWith("#customers"), offenders.toString());
  }

  @Test
  void staysSilentForATypingOnlyJsonSerializeAttribute() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithTypingOnlyAnnotation.class), offenders);

    assertTrue(offenders.isEmpty(), "@JsonSerialize(as=...) refines typing, it replaces nothing");
  }

  @Test
  void staysSilentForAnOverrideOnAComponentThatCarriesNoEncryptedData() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithOverrideOnPlainComponent.class), offenders);

    assertTrue(
        offenders.isEmpty(), "no @Encrypted under the annotated component — nothing to warn");
  }

  @Test
  void theOverrideScanRunsEvenWhenACryptoEngineIsConfigured() {
    // The leak happens precisely when an engine IS wired: validate() returns early on that branch,
    // so the scan must run BEFORE that return or it would never fire in production.
    assertDoesNotThrow(
        () ->
            CryptoConfigValidator.validate(
                List.of(OrderWithPropertySerializerUsing.class), new InMemoryCryptoEngine()));
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithPropertySerializerUsing.class), offenders);
    assertEquals(1, offenders.size());
  }

  // ── Part 2: the override detector must see @Encrypted through the SAME shapes the
  // engine-missing scan already descends through ─────────────────────────────────
  //
  // validate()'s scan() deliberately crosses non-record containers, sealed hierarchies, arrays and
  // generic arguments, because CryptoShreddingModule encrypts @Encrypted fields wherever Jackson
  // reaches them. referencesEncrypted() is the override detector's mirror of that walk, and every
  // test above reaches @Encrypted either directly on the annotated component's record type or one
  // List<> level down. The shapes below are the rest of that walk: if referencesEncrypted() stops
  // short of any of them, the detector stays SILENT on a component whose override writes the PII as
  // plaintext — the exact failure the detector exists to surface.

  /** Non-record container holding an @Encrypted-bearing record. */
  static final class CustomerHolder {
    CustomerWithPii customer;
  }

  record OrderWithOverrideOverNonRecordHolder(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              using = PlaintextCustomerSerializer.class)
          CustomerHolder holder) {}

  enum Tier {
    BASIC,
    GOLD
  }

  /** Non-record container with nothing @Encrypted reachable — primitive, enum and plain record. */
  static final class PlainCatalogHolder {
    String name;
    int count;
    Tier tier;
    PlainSku sku;
  }

  record OrderWithOverrideOverPlainHolder(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(using = PlainSkuSerializer.class)
          PlainCatalogHolder holder) {}

  record OrderWithOverrideOverSealedInterface(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              using = PlaintextCustomerSerializer.class)
          PaymentMethod method) {}

  sealed interface ShippingMethod permits StandardShipping, ExpressShipping {}

  record StandardShipping(int days) implements ShippingMethod {}

  record ExpressShipping(int hours) implements ShippingMethod {}

  record OrderWithOverrideOverPlainSealedInterface(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(using = PlainSkuSerializer.class)
          ShippingMethod shipping) {}

  record OrderWithOverrideOverArrayOfEncrypted(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              contentUsing = PlaintextCustomerSerializer.class)
          CustomerWithPii[] customers) {}

  record OrderWithOverrideOverMapValueOfEncrypted(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              contentUsing = PlaintextCustomerSerializer.class)
          java.util.Map<String, CustomerWithPii> customersByRef) {}

  /** A generic record that itself carries @Encrypted: the PARAMETERIZED type's raw type offends. */
  record AuditedEnvelope<T>(
      String customerId, @Encrypted(subjectId = "customerId") String actorEmail, T payload) {}

  record OrderWithOverrideOverParameterizedEncryptedRecord(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              using = PlaintextCustomerSerializer.class)
          AuditedEnvelope<String> envelope) {}

  /** Self-referential record: the walk must terminate rather than recurse forever. */
  record CategoryNode(String name, CategoryNode child) {}

  record OrderWithOverrideOverRecursiveRecord(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(using = PlainSkuSerializer.class)
          CategoryNode category) {}

  /** The @Encrypted-bearing field is INHERITED, not declared on the component's own type. */
  static class BaseCustomerHolder {
    CustomerWithPii customer;
  }

  static final class DerivedCustomerHolder extends BaseCustomerHolder {}

  record OrderWithOverrideOverInheritedEncrypted(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              using = PlaintextCustomerSerializer.class)
          DerivedCustomerHolder holder) {}

  @Test
  void bothWalksAgreeOnAnEncryptedFieldInHERITEDFromASuperclass() {
    // The two walks in this class are documented to mirror each other. They did not: scan() walks
    // type.getSuperclass(), the override detector only ever called getDeclaredFields(). So
    // validate() correctly refused the type with no engine configured, while the detector
    // stayed SILENT about a property-level override over that same type — the exact case the
    // warning exists for, and the case where the PII is written as plaintext no forget() can reach.
    var withoutEngine =
        assertThrows(
            IllegalStateException.class,
            () ->
                CryptoConfigValidator.validate(
                    List.of(OrderWithOverrideOverInheritedEncrypted.class), null));
    assertTrue(withoutEngine.getMessage().contains("CustomerWithPii"), withoutEngine.getMessage());

    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithOverrideOverInheritedEncrypted.class), offenders);

    assertEquals(
        1,
        offenders.size(),
        "the engine-missing scan sees the inherited @Encrypted field, so the override detector must"
            + " too — otherwise the two walks disagree about the same type");
    assertTrue(offenders.get(0).endsWith("#holder"), offenders.toString());
  }

  /**
   * The override-carrying record is reachable only through an INHERITED field of the registered
   * type — so the walk itself, not just the predicate, has to climb the superclass chain.
   */
  record InnerWithOverride(
      String innerId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              using = PlaintextCustomerSerializer.class)
          CustomerWithPii customer) {}

  static class BaseEventWithInner {
    InnerWithOverride inner;
  }

  static final class DerivedRegisteredEvent extends BaseEventWithInner {}

  @Test
  void findsAnOverrideReachableOnlyThroughAnInheritedFieldOfTheRegisteredType() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(DerivedRegisteredEvent.class), offenders);

    assertEquals(
        1,
        offenders.size(),
        "the registered type declares no fields of its own; the override-carrying record hangs off"
            + " an inherited one, which the walk must follow exactly as scan() does");
    assertTrue(offenders.get(0).endsWith("#customer"), offenders.toString());
  }

  @Test
  void warnsForAnOverrideOverANonRecordHolderOfAnEncryptedRecord() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithOverrideOverNonRecordHolder.class), offenders);

    assertEquals(
        1,
        offenders.size(),
        "the override sits over a non-record holder whose field is an @Encrypted record — the"
            + " detector must descend through it exactly as validate()'s scan does");
    assertTrue(offenders.get(0).endsWith("#holder"), offenders.toString());
  }

  @Test
  void staysSilentForAnOverrideOverANonRecordHolderWithNoEncryptedData() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithOverrideOverPlainHolder.class), offenders);

    assertTrue(
        offenders.isEmpty(),
        "descending through a non-record holder must not warn when nothing under it is @Encrypted");
  }

  @Test
  void warnsForAnOverrideOverASealedInterfaceWhosePermittedRecordIsEncrypted() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithOverrideOverSealedInterface.class), offenders);

    assertEquals(
        1,
        offenders.size(),
        "CardPayment carries @Encrypted and is a permitted subtype of the annotated component's"
            + " sealed interface — the override replaces the serializer for whichever subtype is"
            + " written");
    assertTrue(offenders.get(0).endsWith("#method"), offenders.toString());
  }

  @Test
  void staysSilentForAnOverrideOverASealedInterfaceWithNoEncryptedSubtype() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithOverrideOverPlainSealedInterface.class), offenders);

    assertTrue(offenders.isEmpty(), "no permitted subtype carries @Encrypted — nothing to warn");
  }

  @Test
  void warnsForAnOverrideOverAnArrayOfEncryptedRecords() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithOverrideOverArrayOfEncrypted.class), offenders);

    assertEquals(1, offenders.size(), offenders.toString());
    assertTrue(offenders.get(0).endsWith("#customers"), offenders.toString());
  }

  @Test
  void warnsForAnOverrideOverAMapWhoseValuesAreEncryptedRecords() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithOverrideOverMapValueOfEncrypted.class), offenders);

    assertEquals(
        1,
        offenders.size(),
        "the @Encrypted record is the SECOND type argument — the detector must check every argument,"
            + " not just the first");
    assertTrue(offenders.get(0).endsWith("#customersByRef"), offenders.toString());
  }

  @Test
  void warnsForAnOverrideOverAParameterizedRecordThatItselfCarriesEncrypted() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithOverrideOverParameterizedEncryptedRecord.class), offenders);

    assertEquals(
        1,
        offenders.size(),
        "@Encrypted is on the generic record ITSELF (the parameterized type's raw type), not on any"
            + " type argument");
    assertTrue(offenders.get(0).endsWith("#envelope"), offenders.toString());
  }

  @Test
  void terminatesOnASelfReferentialComponentTypeInsteadOfRecursingForever() {
    var offenders = new java.util.ArrayList<String>();

    assertTimeoutPreemptively(
        java.time.Duration.ofSeconds(5),
        () ->
            CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
                List.of(OrderWithOverrideOverRecursiveRecord.class), offenders));

    assertTrue(offenders.isEmpty(), "nothing under CategoryNode is @Encrypted");
  }

  // ── Which ANNOTATIONS count as a serializer-replacing override ────────────────────

  static final class PlaintextCustomerDeserializer
      extends com.fasterxml.jackson.databind.JsonDeserializer<CustomerWithPii> {
    @Override
    public CustomerWithPii deserialize(
        com.fasterxml.jackson.core.JsonParser p,
        com.fasterxml.jackson.databind.DeserializationContext ctxt) {
      return new CustomerWithPii("c1", "pii@example.com");
    }
  }

  record OrderWithPropertyDeserializerUsing(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonDeserialize(
              using = PlaintextCustomerDeserializer.class)
          CustomerWithPii customer) {}

  /**
   * A NON-Jackson annotation that happens to declare a {@code using} attribute — the shape that
   * separates "is this annotation @JsonSerialize/@JsonDeserialize" from the weaker "does this
   * annotation have a using-like attribute". Plenty of unrelated frameworks use that attribute
   * name.
   */
  @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
  @java.lang.annotation.Target({
    java.lang.annotation.ElementType.FIELD,
    java.lang.annotation.ElementType.METHOD,
    java.lang.annotation.ElementType.PARAMETER
  })
  @interface RenderedWith {
    Class<?> using();
  }

  record OrderWithNonJacksonAnnotationOnComponent(
      String orderId, @RenderedWith(using = PlainSku.class) CustomerWithPii customer) {}

  /**
   * The annotation is declared on the component, but the accessor is declared EXPLICITLY — javac
   * only propagates a component annotation to implicitly declared members, so it lands on the
   * backing field and NOT on the accessor. Detection must read both.
   */
  record OrderWithOverrideOnlyOnTheBackingField(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              using = PlaintextCustomerSerializer.class)
          CustomerWithPii customer) {
    @Override
    public CustomerWithPii customer() {
      return customer;
    }
  }

  @Test
  void detectsAPropertyLevelJsonDeserializeOverrideToo() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithPropertyDeserializerUsing.class), offenders);

    assertEquals(
        1,
        offenders.size(),
        "@JsonDeserialize(using=...) replaces the DECRYPTING deserializer for that property, so the"
            + " record is constructed holding raw ciphertext — the read-side half of the override leak");
    assertTrue(offenders.get(0).endsWith("#customer"), offenders.toString());
  }

  @Test
  void staysSilentForANonJacksonAnnotationOverAnEncryptedBearingComponent() {
    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithNonJacksonAnnotationOnComponent.class), offenders);

    assertTrue(
        offenders.isEmpty(),
        "detection must key off the ANNOTATION TYPE, not merely off an attribute named 'using':"
            + " only @JsonSerialize/@JsonDeserialize replace a Jackson (de)serializer, and a"
            + " false alarm on every unrelated annotation would train operators to ignore the"
            + " warning that does matter");
  }

  @Test
  void detectsAnOverrideThatReachedOnlyTheBackingFieldAndNotTheAccessor() throws Exception {
    // Pin the javac behaviour the detector depends on, so this test cannot silently stop covering
    // the backing-field half of the check if the propagation rules ever change.
    var accessor = OrderWithOverrideOnlyOnTheBackingField.class.getDeclaredMethod("customer");
    var field = OrderWithOverrideOnlyOnTheBackingField.class.getDeclaredField("customer");
    assertNull(
        accessor.getAnnotation(com.fasterxml.jackson.databind.annotation.JsonSerialize.class),
        "an explicitly declared accessor does not receive the component annotation");
    assertNotNull(
        field.getAnnotation(com.fasterxml.jackson.databind.annotation.JsonSerialize.class),
        "the implicitly declared backing field does");

    var offenders = new java.util.ArrayList<String>();
    CryptoConfigValidator.collectJacksonOverridesOverEncryptedTypes(
        List.of(OrderWithOverrideOnlyOnTheBackingField.class), offenders);

    assertEquals(
        1,
        offenders.size(),
        "Jackson still applies the field-level override, so reading only the accessor would miss a"
            + " real plaintext-PII bypass");
    assertTrue(offenders.get(0).endsWith("#customer"), offenders.toString());
  }
}
