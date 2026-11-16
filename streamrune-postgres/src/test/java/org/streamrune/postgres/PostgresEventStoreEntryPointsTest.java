package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.streamrune.core.upcasting.UpcasterChain;

/**
 * {@link PostgresEventStore} is built through {@link PostgresEventStore#builder()}, {@link
 * PostgresEventStore#create} or {@link PostgresEventStoreFactory}. Its constructors take the
 * upcaster chain, a type that is not public API, so none of them is public: a public one would
 * freeze that type with the store.
 */
class PostgresEventStoreEntryPointsTest {

  @Test
  void noConstructorIsPublic() {
    assertThat(PostgresEventStore.class.getConstructors()).isEmpty();
  }

  @Test
  void noPublicOrProtectedMemberNamesTheUpcasterChain() {
    Stream<Executable> members =
        Stream.concat(
            Arrays.stream(PostgresEventStore.class.getDeclaredConstructors()),
            Arrays.stream(PostgresEventStore.class.getDeclaredMethods()));
    assertThat(
            members
                .filter(m -> Modifier.isPublic(m.getModifiers()) || isProtected(m))
                .filter(m -> Arrays.asList(m.getParameterTypes()).contains(UpcasterChain.class))
                .map(PostgresEventStoreEntryPointsTest::describe)
                .toList())
        .isEmpty();
  }

  private static boolean isProtected(Executable member) {
    return Modifier.isProtected(member.getModifiers());
  }

  private static String describe(Executable member) {
    return (member instanceof Constructor<?> ? "constructor" : member.getName())
        + Arrays.toString(member.getParameterTypes());
  }
}
