package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.stream.Stream;
import javax.sql.DataSource;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.types.SubscriptionName;

/**
 * The optional collaborators of the PostgreSQL module's builders and factories (metrics,
 * leadership, health contributor, upcasters) take no {@code null}: each defaults to its empty
 * value, and a {@code null} argument fails at once with a message that names it.
 */
class PostgresNullObjectDefaultsTest {

  static Stream<Arguments> nullCollaborators() {
    return Stream.of(
        refused(
            "healthContributor",
            "HybridEventSubscription.Builder",
            () -> HybridEventSubscription.builder().healthContributor(null)),
        refused(
            "metrics",
            "HybridEventSubscription.Builder",
            () -> HybridEventSubscription.builder().metrics(null)),
        refused(
            "leadership",
            "HybridEventSubscription.Builder",
            () -> HybridEventSubscription.builder().leadership(null)),
        refused(
            "leadership",
            "PostgresNotificationSubscription.Builder",
            () ->
                PostgresNotificationSubscription.builder(SubscriptionName.of("s"))
                    .leadership(null)),
        refused(
            "metrics",
            "PostgresEventStore.Builder",
            () -> PostgresEventStore.builder().metrics(null)),
        refused(
            "upcasters",
            "PostgresEventStore.Builder",
            () -> PostgresEventStore.builder().upcasters(null)),
        refused("metrics", "PostgresEventStoreFactory", () -> factory().metrics(null)),
        refused("upcasters", "PostgresEventStoreFactory", () -> factory().upcasters(null)),
        refused(
            "metrics",
            "PgNotificationListener.Builder",
            () -> PgNotificationListener.builder().metrics(null)),
        refused(
            "metrics",
            "PostgresEventStore.createObjectMapper",
            () -> PostgresEventStore.createObjectMapper(null, null)),
        refused(
            "metrics",
            "PostgresSagaStore.createObjectMapper",
            () -> PostgresSagaStore.createObjectMapper(null, null)),
        refused(
            "metrics",
            "JdbcProjectionRepository.createObjectMapper",
            () -> JdbcProjectionRepository.createObjectMapper(null, null)));
  }

  @ParameterizedTest(name = "{1} refuses a null {0}")
  @MethodSource("nullCollaborators")
  void aNullCollaboratorIsRefused_withItsName(
      String collaborator, String site, ThrowingCallable call) {
    assertThatThrownBy(call)
        .isInstanceOf(NullPointerException.class)
        .hasMessage(collaborator + " is required");
  }

  private static PostgresEventStoreFactory factory() {
    return new PostgresEventStoreFactory(mock(DataSource.class), mock(EventTypeRegistry.class));
  }

  private static Arguments refused(String collaborator, String site, ThrowingCallable call) {
    return Arguments.of(collaborator, site, call);
  }
}
