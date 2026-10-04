package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.ValidationException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.test.InMemoryEventStore;

class VirtualThreadCommandBusValidationTest {

  private static final AggregateType TYPE = AggregateType.of("item");

  ValidatorFactory factory;
  Validator validator;

  // Domain types
  record CreateItem(String id, @NotBlank String name, @Positive int quantity) implements Command {}

  record ItemCreated(String id, String name, int quantity) implements DomainEvent {}

  record ItemState(String id, String name, int quantity)
      implements org.streamrune.core.AggregateState {
    ItemState {}
  }

  // Decider
  static class ItemDecider implements Decider<CreateItem, ItemState, ItemCreated> {
    @Override
    public ItemState initialState() {
      return new ItemState(null, null, 0);
    }

    @Override
    public List<ItemCreated> decide(CreateItem cmd, ItemState state) {
      return List.of(new ItemCreated(cmd.id(), cmd.name(), cmd.quantity()));
    }

    @Override
    public ItemState evolve(ItemState state, ItemCreated event) {
      return new ItemState(event.id(), event.name(), event.quantity());
    }
  }

  @BeforeEach
  void setUp() {
    factory = jakarta.validation.Validation.byDefaultProvider().configure().buildValidatorFactory();
    validator = factory.getValidator();
  }

  @AfterEach
  void tearDown() {
    factory.close();
  }

  private VirtualThreadCommandBus buildBus(InMemoryEventStore eventStore) {
    var interceptor = new BeanValidationInterceptor(validator);
    return VirtualThreadCommandBus.builder()
        .eventStore(eventStore)
        .register(TYPE, CreateItem.class, cmd -> AggregateId.of(cmd.id()), new ItemDecider())
        .interceptors(List.of(interceptor))
        .build();
  }

  @Test
  void shouldValidateCommandBeforeExecution() {
    var eventStore = new InMemoryEventStore();
    var bus = buildBus(eventStore);

    // name=null violates @NotBlank — should throw ValidationException
    var invalidCommand = new CreateItem(UUID.randomUUID().toString(), null, 5);
    var ex = assertThrows(ValidationException.class, () -> bus.execute(invalidCommand));
    assertEquals(1, ex.errors().size());
    assertEquals("name", ex.errors().get(0).field());

    bus.close();
  }

  @Test
  void shouldExecuteValidCommand() {
    var eventStore = new InMemoryEventStore();
    var bus = buildBus(eventStore);

    var validCommand = new CreateItem(UUID.randomUUID().toString(), "Widget", 10);
    var result = bus.execute(validCommand);

    assertEquals(1, result.events().size());
    assertInstanceOf(ItemCreated.class, result.events().get(0));

    bus.close();
  }

  @Test
  void shouldFailFastOnValidationBeforeLocker() {
    var eventStore = new InMemoryEventStore();
    var bus = buildBus(eventStore);

    // quantity=-1 violates @Positive — validation should fail before locking
    var invalidCommand = new CreateItem(UUID.randomUUID().toString(), "Widget", -1);
    var ex = assertThrows(ValidationException.class, () -> bus.execute(invalidCommand));
    assertEquals("quantity", ex.errors().get(0).field());

    bus.close();
  }
}
