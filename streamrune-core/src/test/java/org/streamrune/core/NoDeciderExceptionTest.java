package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;

class NoDeciderExceptionTest {

  @Test
  void noDeciderExceptionShowsHelpfulMessage() {
    var ex =
        new NoDeciderException(
            ProductCommand.class, List.of(OrderCommand.class, CartCommand.class));
    assertTrue(ex.getMessage().contains("ProductCommand"));
    assertTrue(ex.getMessage().contains("Did you forget"));
    assertTrue(ex.getMessage().contains("OrderCommand"));
  }

  // Mini domain for test
  sealed interface ProductCommand {
    record CreateProduct(String id) implements ProductCommand {}
  }

  sealed interface OrderCommand {
    record CreateOrder(String id) implements OrderCommand {}
  }

  sealed interface CartCommand {
    record CreateCart(String id) implements CartCommand {}
  }
}
