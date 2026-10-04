package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Query;
import org.streamrune.core.QueryBus;

class SimpleQueryBusTest {

  private QueryBus queryBus;

  @BeforeEach
  void setUp() {
    queryBus = new SimpleQueryBus();
  }

  @Test
  void shouldDispatchQueryToRegisteredHandler() {
    // Given
    queryBus.register(GetUserById.class, query -> new User(query.id()));

    // When
    User result = queryBus.dispatch(new GetUserById("user-123"));

    // Then
    assertEquals("user-123", result.id());
  }

  @Test
  void shouldThrowWhenNoHandlerRegistered() {
    // Given - no handler registered

    // When/Then
    assertThrows(
        IllegalArgumentException.class, () -> queryBus.dispatch(new GetUserById("user-123")));
  }

  @Test
  void shouldAllowMultipleQueryTypes() {
    // Given
    queryBus.register(GetUserById.class, query -> new User(query.id()));
    queryBus.register(GetAllUsers.class, query -> new User[] {new User("1"), new User("2")});

    // When
    User userResult = queryBus.dispatch(new GetUserById("user-1"));
    User[] allUsers = queryBus.dispatch(new GetAllUsers());

    // Then
    assertEquals("user-1", userResult.id());
    assertEquals(2, allUsers.length);
  }

  // Test query and result types
  record GetUserById(String id) implements Query<User> {}

  record GetAllUsers() implements Query<User[]> {}

  record User(String id) {}
}
