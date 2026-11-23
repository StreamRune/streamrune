package org.streamrune.core.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.audit.EventAuditStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.upcasting.EventUpcaster;

class BackendTypesTest {

  private static final DataSource DATA_SOURCE = stub(DataSource.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Clock CLOCK = Clock.systemUTC();
  private static final EventTypeRegistry REGISTRY = stub(EventTypeRegistry.class);
  private static final EventUpcaster UPCASTER = stub(EventUpcaster.class);
  private static final SnapshotMigration MIGRATION = stub(SnapshotMigration.class);

  @SuppressWarnings("unchecked")
  private static <T> T stub(Class<T> type) {
    return (T)
        Proxy.newProxyInstance(
            BackendTypesTest.class.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, args) ->
                switch (method.getName()) {
                  case "equals" -> proxy == args[0];
                  case "hashCode" -> System.identityHashCode(proxy);
                  case "toString" -> type.getSimpleName() + "Stub";
                  default -> throw new UnsupportedOperationException(method.getName());
                });
  }

  private static BackendContext context(
      DataSource dataSource,
      ObjectMapper mapper,
      Clock clock,
      StreamRuneMetrics metrics,
      StreamRuneTracing tracing) {
    return new BackendContext(dataSource, mapper, clock, metrics, tracing, true, false, true);
  }

  // ── BackendContext ──────────────────────────────────────────────────────────

  @Test
  void backendContextKeepsItsComponents() {
    BackendContext context =
        new BackendContext(
            DATA_SOURCE,
            MAPPER,
            CLOCK,
            StreamRuneMetrics.NOOP,
            StreamRuneTracing.NOOP,
            true,
            false,
            true);

    assertThat(context.dataSource()).isSameAs(DATA_SOURCE);
    assertThat(context.objectMapper()).isSameAs(MAPPER);
    assertThat(context.clock()).isSameAs(CLOCK);
    assertThat(context.metrics()).isSameAs(StreamRuneMetrics.NOOP);
    assertThat(context.tracing()).isSameAs(StreamRuneTracing.NOOP);
    assertThat(context.migrateSchema()).isTrue();
    assertThat(context.migrateReadModelSchema()).isFalse();
    assertThat(context.validateSchema()).isTrue();
  }

  @Test
  void backendContextRejectsNullDataSource() {
    assertThatThrownBy(
            () -> context(null, MAPPER, CLOCK, StreamRuneMetrics.NOOP, StreamRuneTracing.NOOP))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dataSource");
  }

  @Test
  void backendContextRejectsNullObjectMapper() {
    assertThatThrownBy(
            () -> context(DATA_SOURCE, null, CLOCK, StreamRuneMetrics.NOOP, StreamRuneTracing.NOOP))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("objectMapper");
  }

  @Test
  void backendContextRejectsNullClock() {
    assertThatThrownBy(
            () ->
                context(DATA_SOURCE, MAPPER, null, StreamRuneMetrics.NOOP, StreamRuneTracing.NOOP))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("clock");
  }

  @Test
  void backendContextRejectsNullMetrics() {
    assertThatThrownBy(() -> context(DATA_SOURCE, MAPPER, CLOCK, null, StreamRuneTracing.NOOP))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("metrics");
  }

  @Test
  void backendContextRejectsNullTracing() {
    assertThatThrownBy(() -> context(DATA_SOURCE, MAPPER, CLOCK, StreamRuneMetrics.NOOP, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tracing");
  }

  // ── EventStoreOptions ───────────────────────────────────────────────────────

  @Test
  void eventStoreOptionsAcceptsNullForEveryNullableComponent() {
    EventStoreOptions options =
        new EventStoreOptions(REGISTRY, null, List.of(), List.of(), null, null, null, null, null);

    assertThat(options.typeRegistry()).isSameAs(REGISTRY);
    assertThat(options.cryptoEngine()).isNull();
    assertThat(options.upcasters()).isEmpty();
    assertThat(options.snapshotMigrations()).isEmpty();
    assertThat(options.outboxStore()).isNull();
    assertThat(options.outboxEventMapper()).isNull();
    assertThat(options.commandInbox()).isNull();
    assertThat(options.eventAuditStore()).isNull();
    assertThat(options.statementTimeout()).isNull();
  }

  @Test
  void eventStoreOptionsKeepsEveryComponent() {
    CryptoEngine crypto = stub(CryptoEngine.class);
    OutboxStore outbox = stub(OutboxStore.class);
    OutboxEventMapper mapper = stub(OutboxEventMapper.class);
    CommandInbox inbox = stub(CommandInbox.class);
    EventAuditStore audit = stub(EventAuditStore.class);

    EventStoreOptions options =
        new EventStoreOptions(
            REGISTRY,
            crypto,
            List.of(UPCASTER),
            List.of(MIGRATION),
            outbox,
            mapper,
            inbox,
            audit,
            Duration.ofSeconds(5));

    assertThat(options.cryptoEngine()).isSameAs(crypto);
    assertThat(options.upcasters()).containsExactly(UPCASTER);
    assertThat(options.snapshotMigrations()).containsExactly(MIGRATION);
    assertThat(options.outboxStore()).isSameAs(outbox);
    assertThat(options.outboxEventMapper()).isSameAs(mapper);
    assertThat(options.commandInbox()).isSameAs(inbox);
    assertThat(options.eventAuditStore()).isSameAs(audit);
    assertThat(options.statementTimeout()).isEqualTo(Duration.ofSeconds(5));
  }

  @Test
  void eventStoreOptionsAcceptsAZeroStatementTimeout() {
    EventStoreOptions options =
        new EventStoreOptions(
            REGISTRY, null, List.of(), List.of(), null, null, null, null, Duration.ZERO);

    assertThat(options.statementTimeout()).isEqualTo(Duration.ZERO);
  }

  @Test
  void eventStoreOptionsCopiesItsLists() {
    List<EventUpcaster> upcasters = new ArrayList<>(List.of(UPCASTER));
    List<SnapshotMigration> migrations = new ArrayList<>(List.of(MIGRATION));

    EventStoreOptions options =
        new EventStoreOptions(REGISTRY, null, upcasters, migrations, null, null, null, null, null);
    upcasters.clear();
    migrations.clear();

    assertThat(options.upcasters()).containsExactly(UPCASTER);
    assertThat(options.snapshotMigrations()).containsExactly(MIGRATION);
    assertThatThrownBy(() -> options.upcasters().add(UPCASTER))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void eventStoreOptionsWithEqualComponentsAreEqual() {
    EventStoreOptions first =
        new EventStoreOptions(
            REGISTRY,
            null,
            new ArrayList<>(List.of(UPCASTER)),
            List.of(),
            null,
            null,
            null,
            null,
            null);
    EventStoreOptions second =
        new EventStoreOptions(
            REGISTRY, null, List.of(UPCASTER), List.of(), null, null, null, null, null);

    assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
  }

  @Test
  void eventStoreOptionsRejectsNullTypeRegistry() {
    assertThatThrownBy(
            () ->
                new EventStoreOptions(
                    null, null, List.of(), List.of(), null, null, null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("typeRegistry");
  }

  @Test
  void eventStoreOptionsRejectsNullUpcasters() {
    assertThatThrownBy(
            () ->
                new EventStoreOptions(
                    REGISTRY, null, null, List.of(), null, null, null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("upcasters");
  }

  @Test
  void eventStoreOptionsRejectsANullUpcaster() {
    List<EventUpcaster> upcasters = Arrays.asList(UPCASTER, null);

    assertThatThrownBy(
            () ->
                new EventStoreOptions(
                    REGISTRY, null, upcasters, List.of(), null, null, null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("upcasters");
  }

  @Test
  void eventStoreOptionsRejectsNullSnapshotMigrations() {
    assertThatThrownBy(
            () ->
                new EventStoreOptions(
                    REGISTRY, null, List.of(), null, null, null, null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("snapshotMigrations");
  }

  @Test
  void eventStoreOptionsRejectsANullSnapshotMigration() {
    List<SnapshotMigration> migrations = Arrays.asList(null, MIGRATION);

    assertThatThrownBy(
            () ->
                new EventStoreOptions(
                    REGISTRY, null, List.of(), migrations, null, null, null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("snapshotMigrations");
  }

  @Test
  void eventStoreOptionsRejectsANegativeStatementTimeout() {
    assertThatThrownBy(
            () ->
                new EventStoreOptions(
                    REGISTRY,
                    null,
                    List.of(),
                    List.of(),
                    null,
                    null,
                    null,
                    null,
                    Duration.ofMillis(-1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("statementTimeout");
  }

  // ── OutboxOptions ───────────────────────────────────────────────────────────

  @Test
  void outboxOptionsKeepsItsComponents() {
    OutboxOptions options =
        new OutboxOptions(Duration.ofMinutes(4), OutboxOrderingMode.AVAILABILITY_FIRST);

    assertThat(options.claimLease()).isEqualTo(Duration.ofMinutes(4));
    assertThat(options.orderingMode()).isEqualTo(OutboxOrderingMode.AVAILABILITY_FIRST);
  }

  @Test
  void outboxOptionsRejectsNullClaimLease() {
    assertThatThrownBy(() -> new OutboxOptions(null, OutboxOrderingMode.STRICT_PER_AGGREGATE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("claimLease");
  }

  @Test
  void outboxOptionsRejectsAZeroClaimLease() {
    assertThatThrownBy(
            () -> new OutboxOptions(Duration.ZERO, OutboxOrderingMode.STRICT_PER_AGGREGATE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("claimLease");
  }

  @Test
  void outboxOptionsRejectsANegativeClaimLease() {
    assertThatThrownBy(
            () ->
                new OutboxOptions(Duration.ofSeconds(-1), OutboxOrderingMode.STRICT_PER_AGGREGATE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("claimLease");
  }

  @Test
  void outboxOptionsRejectsNullOrderingMode() {
    assertThatThrownBy(() -> new OutboxOptions(Duration.ofMinutes(4), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("orderingMode");
  }

  // ── BackendStores ───────────────────────────────────────────────────────────

  @Test
  void backendStoresDefaultCloseDoesNothing() {
    BackendStores stores =
        (BackendStores)
            Proxy.newProxyInstance(
                BackendTypesTest.class.getClassLoader(),
                new Class<?>[] {BackendStores.class},
                (proxy, method, args) -> {
                  if (method.isDefault()) {
                    return java.lang.reflect.InvocationHandler.invokeDefault(proxy, method, args);
                  }
                  throw new UnsupportedOperationException(method.getName());
                });

    assertThatCode(stores::close).doesNotThrowAnyException();
  }
}
