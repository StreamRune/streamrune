package org.streamrune.core.backend;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationHandler;
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
  private static final CryptoEngine CRYPTO = stub(CryptoEngine.class);
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
      CryptoEngine cryptoEngine,
      Clock clock,
      StreamRuneMetrics metrics,
      StreamRuneTracing tracing) {
    return new BackendContext(dataSource, cryptoEngine, clock, metrics, tracing, true, false, true);
  }

  private static EventStoreOptions options(
      List<EventUpcaster> upcasters,
      List<SnapshotMigration> migrations,
      Duration statementTimeout) {
    return new EventStoreOptions(
        REGISTRY, upcasters, migrations, null, null, null, null, statementTimeout);
  }

  // ── BackendContext ──────────────────────────────────────────────────────────

  @Test
  void backendContextKeepsItsComponents() {
    BackendContext context =
        new BackendContext(
            DATA_SOURCE,
            CRYPTO,
            CLOCK,
            StreamRuneMetrics.NOOP,
            StreamRuneTracing.NOOP,
            true,
            false,
            true);

    assertThat(context.dataSource()).isSameAs(DATA_SOURCE);
    assertThat(context.cryptoEngine()).isSameAs(CRYPTO);
    assertThat(context.clock()).isSameAs(CLOCK);
    assertThat(context.metrics()).isSameAs(StreamRuneMetrics.NOOP);
    assertThat(context.tracing()).isSameAs(StreamRuneTracing.NOOP);
    assertThat(context.migrateSchema()).isTrue();
    assertThat(context.migrateReadModelSchema()).isFalse();
    assertThat(context.validateSchema()).isTrue();
  }

  @Test
  void backendContextAcceptsNoCryptoEngine() {
    BackendContext context =
        context(DATA_SOURCE, null, CLOCK, StreamRuneMetrics.NOOP, StreamRuneTracing.NOOP);

    assertThat(context.cryptoEngine()).isNull();
  }

  @Test
  void backendContextRejectsNullDataSource() {
    assertThatThrownBy(
            () -> context(null, CRYPTO, CLOCK, StreamRuneMetrics.NOOP, StreamRuneTracing.NOOP))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("dataSource");
  }

  @Test
  void backendContextRejectsNullClock() {
    assertThatThrownBy(
            () ->
                context(DATA_SOURCE, CRYPTO, null, StreamRuneMetrics.NOOP, StreamRuneTracing.NOOP))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("clock");
  }

  @Test
  void backendContextRejectsNullMetrics() {
    assertThatThrownBy(() -> context(DATA_SOURCE, CRYPTO, CLOCK, null, StreamRuneTracing.NOOP))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("metrics");
  }

  @Test
  void backendContextRejectsNullTracing() {
    assertThatThrownBy(() -> context(DATA_SOURCE, CRYPTO, CLOCK, StreamRuneMetrics.NOOP, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("tracing");
  }

  // ── EventStoreOptions ───────────────────────────────────────────────────────

  @Test
  void eventStoreOptionsAcceptsNullForEveryNullableComponent() {
    EventStoreOptions options =
        new EventStoreOptions(REGISTRY, List.of(), List.of(), null, null, null, null, null);

    assertThat(options.typeRegistry()).isSameAs(REGISTRY);
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
    OutboxStore outbox = stub(OutboxStore.class);
    OutboxEventMapper mapper = stub(OutboxEventMapper.class);
    CommandInbox inbox = stub(CommandInbox.class);
    EventAuditStore audit = stub(EventAuditStore.class);

    EventStoreOptions options =
        new EventStoreOptions(
            REGISTRY,
            List.of(UPCASTER),
            List.of(MIGRATION),
            outbox,
            mapper,
            inbox,
            audit,
            Duration.ofSeconds(5));

    assertThat(options.upcasters()).containsExactly(UPCASTER);
    assertThat(options.snapshotMigrations()).containsExactly(MIGRATION);
    assertThat(options.outboxStore()).isSameAs(outbox);
    assertThat(options.outboxEventMapper()).isSameAs(mapper);
    assertThat(options.commandInbox()).isSameAs(inbox);
    assertThat(options.eventAuditStore()).isSameAs(audit);
    assertThat(options.statementTimeout()).isEqualTo(Duration.ofSeconds(5));
  }

  @Test
  void eventStoreOptionsLeavesTheStatementTimeoutRuleToTheBackend() {
    // Core keeps any value, a negative one included: the backend refuses what it cannot honour
    // with a BackendOptionException naming "statementTimeout".
    assertThat(options(List.of(), List.of(), Duration.ZERO).statementTimeout())
        .isEqualTo(Duration.ZERO);
    assertThat(options(List.of(), List.of(), Duration.ofMillis(-1)).statementTimeout())
        .isEqualTo(Duration.ofMillis(-1));
  }

  @Test
  void eventStoreOptionsCopiesItsLists() {
    List<EventUpcaster> upcasters = new ArrayList<>(List.of(UPCASTER));
    List<SnapshotMigration> migrations = new ArrayList<>(List.of(MIGRATION));

    EventStoreOptions options = options(upcasters, migrations, null);
    upcasters.clear();
    migrations.clear();

    assertThat(options.upcasters()).containsExactly(UPCASTER);
    assertThat(options.snapshotMigrations()).containsExactly(MIGRATION);
    assertThatThrownBy(() -> options.upcasters().add(UPCASTER))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void eventStoreOptionsWithEqualComponentsAreEqual() {
    EventStoreOptions first = options(new ArrayList<>(List.of(UPCASTER)), List.of(), null);
    EventStoreOptions second = options(List.of(UPCASTER), List.of(), null);

    assertThat(first).isEqualTo(second).hasSameHashCodeAs(second);
  }

  @Test
  void eventStoreOptionsRejectsNullTypeRegistry() {
    assertThatThrownBy(
            () -> new EventStoreOptions(null, List.of(), List.of(), null, null, null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("typeRegistry");
  }

  @Test
  void eventStoreOptionsRejectsNullUpcasters() {
    assertThatThrownBy(() -> options(null, List.of(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("upcasters");
  }

  @Test
  void eventStoreOptionsRejectsANullUpcaster() {
    List<EventUpcaster> upcasters = Arrays.asList(UPCASTER, null);

    assertThatThrownBy(() -> options(upcasters, List.of(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("upcasters");
  }

  @Test
  void eventStoreOptionsRejectsNullSnapshotMigrations() {
    assertThatThrownBy(() -> options(List.of(), null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("snapshotMigrations");
  }

  @Test
  void eventStoreOptionsRejectsANullSnapshotMigration() {
    List<SnapshotMigration> migrations = Arrays.asList(null, MIGRATION);

    assertThatThrownBy(() -> options(List.of(), migrations, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("snapshotMigrations");
  }

  // ── BackendOptionException ──────────────────────────────────────────────────

  @Test
  void backendOptionExceptionNamesTheOption() {
    BackendOptionException exception =
        new BackendOptionException("statementTimeout", "must be at least 1ms, got: PT0.0005S");

    assertThat(exception)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("must be at least 1ms, got: PT0.0005S")
        .hasNoCause();
    assertThat(exception.option()).isEqualTo("statementTimeout");
  }

  @Test
  void backendOptionExceptionKeepsItsCause() {
    IllegalArgumentException cause = new IllegalArgumentException("too short");

    BackendOptionException exception =
        new BackendOptionException("statementTimeout", "too short", cause);

    assertThat(exception.option()).isEqualTo("statementTimeout");
    assertThat(exception).hasMessage("too short").hasCause(cause);
  }

  @Test
  void backendOptionExceptionRejectsAMissingOption() {
    assertThatThrownBy(() -> new BackendOptionException(null, "message"))
        .isInstanceOf(IllegalArgumentException.class)
        .isNotInstanceOf(BackendOptionException.class)
        .hasMessageContaining("option");
    assertThatThrownBy(() -> new BackendOptionException(" ", "message", null))
        .isInstanceOf(IllegalArgumentException.class)
        .isNotInstanceOf(BackendOptionException.class)
        .hasMessageContaining("option");
  }

  // ── OutboxOptions ───────────────────────────────────────────────────────────

  @Test
  void outboxOptionsKeepsItsComponents() {
    OutboxOptions options =
        new OutboxOptions(Duration.ofMinutes(2), OutboxOrderingMode.AVAILABILITY_FIRST);

    assertThat(options.claimLease()).isEqualTo(Duration.ofMinutes(2));
    assertThat(options.orderingMode()).isEqualTo(OutboxOrderingMode.AVAILABILITY_FIRST);
  }

  @Test
  void outboxOptionsDefaultClaimLeaseIsFourMinutes() {
    assertThat(OutboxOptions.DEFAULT_CLAIM_LEASE).isEqualTo(Duration.ofMinutes(4));
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
    assertThatThrownBy(() -> new OutboxOptions(OutboxOptions.DEFAULT_CLAIM_LEASE, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("orderingMode");
  }

  // ── BackendStores ───────────────────────────────────────────────────────────

  @Test
  void backendStoresCloseThrowsNoCheckedException() {
    boolean[] reached = {false};
    // No catch clause: this compiles only while BackendStores.close() declares no checked
    // exception, so a try-with-resources over the bundle needs no handler.
    try (BackendStores stores = defaultMethodsOnly()) {
      reached[0] = stores != null;
    }

    assertThat(reached[0]).isTrue();
  }

  private static BackendStores defaultMethodsOnly() {
    return (BackendStores)
        Proxy.newProxyInstance(
            BackendTypesTest.class.getClassLoader(),
            new Class<?>[] {BackendStores.class},
            (proxy, method, args) -> {
              if (method.isDefault()) {
                return InvocationHandler.invokeDefault(proxy, method, args);
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }
}
