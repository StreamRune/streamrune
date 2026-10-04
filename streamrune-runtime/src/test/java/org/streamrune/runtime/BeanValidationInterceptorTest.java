package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import java.time.Instant;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.ValidationException;
import org.streamrune.core.ValidationGroups;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;

class BeanValidationInterceptorTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  ValidatorFactory factory;
  Validator validator;

  @BeforeEach
  void setUp() {
    factory = jakarta.validation.Validation.byDefaultProvider().configure().buildValidatorFactory();
    validator = factory.getValidator();
  }

  @AfterEach
  void tearDown() {
    factory.close();
  }

  // Test command types
  record ValidCommand(@NotBlank String name, @Positive int quantity) implements Command {}

  record EmptyCommand() implements Command {}

  @Test
  void shouldPassValidationForValidCommand() {
    var interceptor = new BeanValidationInterceptor(validator);
    var ctx =
        new CommandContext(
            new ValidCommand("widget", 5),
            "ValidCommand",
            null,
            TYPE,
            AggregateId.of("agg-1"),
            null,
            Instant.now());

    boolean result = interceptor.before(ctx);
    assertTrue(result, "Valid command should pass validation");
  }

  @Test
  void shouldThrowValidationExceptionForInvalidCommand() {
    var interceptor = new BeanValidationInterceptor(validator);
    // name is null — violates @NotBlank
    var ctx =
        new CommandContext(
            new ValidCommand(null, 5),
            "ValidCommand",
            null,
            TYPE,
            AggregateId.of("agg-1"),
            null,
            Instant.now());

    var ex = assertThrows(ValidationException.class, () -> interceptor.before(ctx));
    assertEquals(1, ex.errors().size());
    assertEquals("name", ex.errors().get(0).field());
  }

  @Test
  void shouldCollectMultipleViolations() {
    var interceptor = new BeanValidationInterceptor(validator);
    // name=null violates @NotBlank, quantity=-1 violates @Positive
    var ctx =
        new CommandContext(
            new ValidCommand(null, -1),
            "ValidCommand",
            null,
            TYPE,
            AggregateId.of("agg-1"),
            null,
            Instant.now());

    var ex = assertThrows(ValidationException.class, () -> interceptor.before(ctx));
    assertEquals(2, ex.errors().size());
  }

  @Test
  void shouldReturnTrueForCommandWithNoConstraints() {
    var interceptor = new BeanValidationInterceptor(validator);
    var ctx =
        new CommandContext(
            new EmptyCommand(),
            "EmptyCommand",
            null,
            TYPE,
            AggregateId.of("agg-1"),
            null,
            Instant.now());

    boolean result = interceptor.before(ctx);
    assertTrue(result, "Command without constraints should pass");
  }

  @Test
  void shouldReturnTrueWhenValidatorIsNull() {
    var interceptor = new BeanValidationInterceptor(null);
    var ctx =
        new CommandContext(
            new ValidCommand(null, -1),
            "ValidCommand",
            null,
            TYPE,
            AggregateId.of("agg-1"),
            null,
            Instant.now());

    boolean result = interceptor.before(ctx);
    assertTrue(result, "Null validator should be a no-op");
  }

  @Test
  void shouldReturnTrueForNullCommand() {
    var interceptor = new BeanValidationInterceptor(validator);
    var ctx =
        new CommandContext(
            null, "NullCommand", null, TYPE, AggregateId.of("agg-1"), null, Instant.now());

    boolean result = interceptor.before(ctx);
    assertTrue(result, "Null command should pass validation");
  }

  // --- Validation groups support ---

  interface CreateGroup {}

  @ValidationGroups(CreateGroup.class)
  record GroupedCommand(
      @NotBlank(groups = CreateGroup.class) String name,
      @Positive int quantity // Default group only
      ) implements Command {}

  @Test
  void shouldValidateOnlySpecifiedGroups() {
    var interceptor = new BeanValidationInterceptor(validator);
    // name is blank — violates @NotBlank in CreateGroup → should fail
    // quantity is -1 — violates @Positive in Default group → should NOT be checked
    var ctx =
        new CommandContext(
            new GroupedCommand("", -1),
            "GroupedCommand",
            null,
            TYPE,
            AggregateId.of("agg-1"),
            null,
            Instant.now());

    var ex = assertThrows(ValidationException.class, () -> interceptor.before(ctx));
    assertEquals(1, ex.errors().size());
    assertEquals("name", ex.errors().get(0).field());
  }

  /** The same constraints as {@link GroupedCommand}, without {@code @ValidationGroups}. */
  record UngroupedCommand(
      @NotBlank(groups = CreateGroup.class) String name,
      @Positive int quantity // Default group only
      ) implements Command {}

  @Test
  void shouldUseDefaultGroupWhenNoAnnotation() {
    var interceptor = new BeanValidationInterceptor(validator);
    // name is blank — violates @NotBlank in CreateGroup → not checked without the annotation
    // quantity is -1 — violates @Positive in the Default group → should fail
    var ctx =
        new CommandContext(
            new UngroupedCommand("", -1),
            "UngroupedCommand",
            null,
            TYPE,
            AggregateId.of("agg-1"),
            null,
            Instant.now());

    var ex = assertThrows(ValidationException.class, () -> interceptor.before(ctx));
    assertEquals(1, ex.errors().size());
    assertEquals("quantity", ex.errors().get(0).field());
  }

  // --- Custom validator support ---

  @java.lang.annotation.Target({java.lang.annotation.ElementType.FIELD})
  @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
  @jakarta.validation.Constraint(validatedBy = EvenNumberValidator.class)
  @interface EvenNumber {
    String message() default "Must be even";

    Class<?>[] groups() default {};

    Class<? extends jakarta.validation.Payload>[] payload() default {};
  }

  public static class EvenNumberValidator
      implements jakarta.validation.ConstraintValidator<EvenNumber, Integer> {
    @Override
    public boolean isValid(Integer value, jakarta.validation.ConstraintValidatorContext ctx) {
      return value != null && value % 2 == 0;
    }
  }

  record CustomValidatedCommand(@EvenNumber int count) implements Command {}

  @Test
  void shouldSupportCustomConstraintValidator() {
    var interceptor = new BeanValidationInterceptor(validator);
    var ctx =
        new CommandContext(
            new CustomValidatedCommand(3),
            "CustomValidatedCommand",
            null,
            TYPE,
            AggregateId.of("agg-1"),
            null,
            Instant.now());

    var ex = assertThrows(ValidationException.class, () -> interceptor.before(ctx));
    assertEquals(1, ex.errors().size());
    assertEquals("count", ex.errors().get(0).field());
    assertEquals("Must be even", ex.errors().get(0).message());
    assertEquals("EvenNumber", ex.errors().get(0).code());
  }

  // --- Cross-field validation support ---

  @java.lang.annotation.Target(java.lang.annotation.ElementType.TYPE)
  @java.lang.annotation.Retention(java.lang.annotation.RetentionPolicy.RUNTIME)
  @jakarta.validation.Constraint(validatedBy = StartBeforeEndValidator.class)
  @interface StartBeforeEnd {
    String message() default "Start must be before end";

    Class<?>[] groups() default {};

    Class<? extends jakarta.validation.Payload>[] payload() default {};
  }

  public static class StartBeforeEndValidator
      implements jakarta.validation.ConstraintValidator<StartBeforeEnd, DateRangeCommand> {
    @Override
    public boolean isValid(
        DateRangeCommand cmd, jakarta.validation.ConstraintValidatorContext ctx) {
      return cmd.start() < cmd.end();
    }
  }

  @StartBeforeEnd
  record DateRangeCommand(int start, int end) implements Command {}

  @Test
  void shouldSupportCrossFieldClassLevelConstraint() {
    var interceptor = new BeanValidationInterceptor(validator);
    // start=10, end=5 → invalid
    var ctx =
        new CommandContext(
            new DateRangeCommand(10, 5),
            "DateRangeCommand",
            null,
            TYPE,
            AggregateId.of("agg-1"),
            null,
            Instant.now());

    var ex = assertThrows(ValidationException.class, () -> interceptor.before(ctx));
    assertEquals(1, ex.errors().size());
    // Class-level constraint has empty property path
    assertEquals("", ex.errors().get(0).field());
    assertEquals("Start must be before end", ex.errors().get(0).message());
    assertEquals("StartBeforeEnd", ex.errors().get(0).code());
  }
}
