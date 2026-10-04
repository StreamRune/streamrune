package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.DatabaseMetaData;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.SnapshotPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.StreamRune;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Full integration test exercising the StreamRune builder, CartDecider, PostgreSQL event store, and
 * ScopedValue context propagation.
 */
@Testcontainers
class CartIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  static PGSimpleDataSource dataSource;
  static ObjectMapper objectMapper;
  static EventTypeRegistry typeRegistry;

  // === Cart Domain ===

  sealed interface CartCommand extends org.streamrune.core.Command {
    record AddItem(String cartId, String sku, int qty) implements CartCommand {
      public AddItem {
        if (qty <= 0) throw new IllegalArgumentException("Qty must be > 0");
        if (cartId == null || cartId.isBlank())
          throw new IllegalArgumentException("Invalid cart ID");
        if (sku == null || sku.isBlank()) throw new IllegalArgumentException("Invalid SKU");
      }
    }

    record RemoveItem(String cartId, String sku) implements CartCommand {
      public RemoveItem {
        if (cartId == null || cartId.isBlank())
          throw new IllegalArgumentException("Invalid cart ID");
        if (sku == null || sku.isBlank()) throw new IllegalArgumentException("Invalid SKU");
      }
    }

    record Checkout(String cartId) implements CartCommand {
      public Checkout {
        if (cartId == null || cartId.isBlank())
          throw new IllegalArgumentException("Invalid cart ID");
      }
    }
  }

  sealed interface CartEvent extends DomainEvent {
    record ItemAdded(String sku, int qty) implements CartEvent {}

    record ItemRemoved(String sku) implements CartEvent {}

    record CheckedOut() implements CartEvent {}
  }

  record CartState(Map<String, Integer> items, boolean isClosed) implements AggregateState {
    public CartState() {
      this(Map.of(), false);
    }
  }

  static final class CartDecider implements Decider<CartCommand, CartState, CartEvent> {

    @Override
    public CartState initialState() {
      return new CartState();
    }

    @Override
    public List<CartEvent> decide(CartCommand cmd, CartState state) {
      return switch (cmd) {
        case CartCommand.AddItem c -> {
          if (state.isClosed()) throw new DomainException("Cart is closed");
          yield List.of(new CartEvent.ItemAdded(c.sku(), c.qty()));
        }
        case CartCommand.RemoveItem c -> {
          if (state.isClosed()) throw new DomainException("Cart is closed");
          yield List.of(new CartEvent.ItemRemoved(c.sku()));
        }
        case CartCommand.Checkout _ -> {
          if (state.isClosed()) throw new DomainException("Cart is already closed");
          yield List.of(new CartEvent.CheckedOut());
        }
      };
    }

    @Override
    public CartState evolve(CartState state, CartEvent evt) {
      return switch (evt) {
        case CartEvent.ItemAdded e -> {
          var items = new HashMap<>(state.items());
          items.merge(e.sku(), e.qty(), Integer::sum);
          yield new CartState(Map.copyOf(items), false);
        }
        case CartEvent.ItemRemoved e -> {
          var items = new HashMap<>(state.items());
          items.remove(e.sku());
          yield new CartState(Map.copyOf(items), false);
        }
        case CartEvent.CheckedOut _ -> new CartState(state.items(), true);
      };
    }
  }

  // === Setup ===

  @BeforeAll
  static void initSchema() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());

    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return switch (eventType.name()) {
              case "ItemAdded" -> CartEvent.ItemAdded.class;
              case "ItemRemoved" -> CartEvent.ItemRemoved.class;
              case "CheckedOut" -> CartEvent.CheckedOut.class;
              default -> throw new IllegalArgumentException("Unknown event type: " + eventType);
            };
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return CartState.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return java.util.List.of(
                CartEvent.ItemAdded.class,
                CartEvent.ItemRemoved.class,
                CartEvent.CheckedOut.class,
                CartState.class);
          }
        };

    // Run Flyway migrations via factory
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  @BeforeEach
  void cleanTables() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("DELETE FROM snapshot_store");
      stmt.execute("DELETE FROM projection_offset");
    }
  }

  // === Tests ===

  @Test
  void fullCartFlowWithPostgresEventStore() {
    var store =
        PostgresEventStore.builder().dataSource(dataSource).typeRegistry(typeRegistry).build();

    var streamRune =
        StreamRune.builder()
            .eventStore(store)
            .register(
                TestStreams.TYPE,
                CartCommand.class,
                cmd ->
                    switch (cmd) {
                      case CartCommand.AddItem c -> AggregateId.of(c.cartId());
                      case CartCommand.RemoveItem c -> AggregateId.of(c.cartId());
                      case CartCommand.Checkout c -> AggregateId.of(c.cartId());
                    },
                new CartDecider())
            .retryPolicy(RetryPolicy.DEFAULT)
            .snapshotPolicy(SnapshotPolicy.everyNEvents(100))
            .build();

    var ctx =
        new StreamRuneContext.RequestContext(
            TraceId.of(UUID.randomUUID().toString()),
            UserId.of("test-user"),
            CorrelationId.of(UUID.randomUUID().toString()),
            Instant.now(),
            Map.of());

    ScopedValue.where(StreamRuneContext.CURRENT, ctx)
        .run(
            () -> {
              // Execute commands: AddItem, AddItem, Checkout
              streamRune.execute(new CartCommand.AddItem("cart-1", "SKU-42", 3));
              streamRune.execute(new CartCommand.AddItem("cart-1", "SKU-99", 1));
              streamRune.execute(new CartCommand.Checkout("cart-1"));
            });

    // Verify: 3 events stored, version=3
    var history = store.load(TestStreams.stream("cart-1"));
    assertEquals(3, history.version().value());
    assertEquals(3, history.events().size());

    // Verify event types in order
    assertInstanceOf(CartEvent.ItemAdded.class, history.events().get(0).event());
    assertInstanceOf(CartEvent.ItemAdded.class, history.events().get(1).event());
    assertInstanceOf(CartEvent.CheckedOut.class, history.events().get(2).event());

    // Verify event content
    var firstAdd = (CartEvent.ItemAdded) history.events().get(0).event();
    assertEquals("SKU-42", firstAdd.sku());
    assertEquals(3, firstAdd.qty());

    var secondAdd = (CartEvent.ItemAdded) history.events().get(1).event();
    assertEquals("SKU-99", secondAdd.sku());
    assertEquals(1, secondAdd.qty());

    // Verify metadata propagation from ScopedValue context
    var metadata = history.events().get(0).metadata();
    assertEquals(ctx.correlationId().value(), metadata.correlationId().value());
    assertEquals("test-user", metadata.userId().value());
  }

  @Test
  void closedCartRejectsNewItem() {
    var store =
        PostgresEventStore.builder().dataSource(dataSource).typeRegistry(typeRegistry).build();

    var streamRune =
        StreamRune.builder()
            .eventStore(store)
            .register(
                TestStreams.TYPE,
                CartCommand.class,
                cmd ->
                    switch (cmd) {
                      case CartCommand.AddItem c -> AggregateId.of(c.cartId());
                      case CartCommand.RemoveItem c -> AggregateId.of(c.cartId());
                      case CartCommand.Checkout c -> AggregateId.of(c.cartId());
                    },
                new CartDecider())
            .retryPolicy(RetryPolicy.DEFAULT)
            .snapshotPolicy(SnapshotPolicy.everyNEvents(100))
            .build();

    var ctx =
        new StreamRuneContext.RequestContext(
            TraceId.of(UUID.randomUUID().toString()),
            UserId.of("test-user"),
            CorrelationId.of(UUID.randomUUID().toString()),
            Instant.now(),
            Map.of());

    // First, add item and checkout
    ScopedValue.where(StreamRuneContext.CURRENT, ctx)
        .run(
            () -> {
              streamRune.execute(new CartCommand.AddItem("cart-2", "SKU-42", 1));
              streamRune.execute(new CartCommand.Checkout("cart-2"));
            });

    // Then, attempt to add item to closed cart
    var ex =
        assertThrows(
            DomainException.class,
            () ->
                ScopedValue.where(StreamRuneContext.CURRENT, ctx)
                    .run(() -> streamRune.execute(new CartCommand.AddItem("cart-2", "SKU-99", 1))));

    assertEquals("Cart is closed", ex.getMessage());
  }

  @Test
  void snapshot_store_has_snapshot_version_column() throws Exception {
    try (var conn = dataSource.getConnection();
        var rs =
            conn.getMetaData().getColumns(null, "public", "snapshot_store", "snapshot_version")) {
      assertTrue(rs.next(), "snapshot_version column should exist in snapshot_store");
      // int4 or integer depending on driver
      String typeName = rs.getString("TYPE_NAME").toLowerCase();
      assertTrue(
          typeName.contains("int"), "snapshot_version should be an integer type, got: " + typeName);
      assertEquals(
          DatabaseMetaData.columnNoNulls,
          rs.getInt("NULLABLE"),
          "snapshot_version should be NOT NULL");
      // COLUMN_DEF may include whitespace or quoting depending on driver; just verify it contains
      // "1"
      String colDefault = rs.getString("COLUMN_DEF");
      assertNotNull(colDefault, "snapshot_version should have a default value");
      assertTrue(
          colDefault.trim().contains("1"),
          "snapshot_version default should be 1, got: " + colDefault);
    }
  }

  @Test
  void loadWithVersion_ignoresSnapshot_whenVersionMismatches() throws Exception {
    StreamId streamId = TestStreams.stream("snapshot-version-test-" + UUID.randomUUID());
    EventStore store =
        PostgresEventStore.builder().dataSource(dataSource).typeRegistry(typeRegistry).build();

    // Append one event
    var result = store.append(streamId, List.of(makeEnvelope(streamId, 1)), Version.initial());

    // Save snapshot at schema version 1
    store.saveSnapshot(streamId, result.finalVersion(), new CartState(), 1);

    // Load with expected version 2 (mismatch)
    AggregateHistory history = store.load(streamId, 2);

    // Snapshot should be ignored
    assertNull(history.snapshotState(), "Snapshot should be ignored on version mismatch");
    assertFalse(history.events().isEmpty(), "All events should be loaded when snapshot is ignored");
  }

  @Test
  void loadWithVersion_usesSnapshot_whenVersionMatches() throws Exception {
    StreamId streamId = TestStreams.stream("snapshot-version-match-" + UUID.randomUUID());
    EventStore store =
        PostgresEventStore.builder().dataSource(dataSource).typeRegistry(typeRegistry).build();

    var result = store.append(streamId, List.of(makeEnvelope(streamId, 1)), Version.initial());

    CartState savedState = new CartState();
    store.saveSnapshot(streamId, result.finalVersion(), savedState, 1);

    // Load with matching version 1
    AggregateHistory history = store.load(streamId, 1);

    assertNotNull(history.snapshotState(), "Snapshot should be used on version match");
    assertTrue(history.events().isEmpty(), "No events should be returned after snapshot");
  }

  @Test
  void saveSnapshot_withVersion_storesSnapshotVersion() throws Exception {
    StreamId streamId = TestStreams.stream("snapshot-save-version-" + UUID.randomUUID());
    EventStore store =
        PostgresEventStore.builder().dataSource(dataSource).typeRegistry(typeRegistry).build();

    var result = store.append(streamId, List.of(makeEnvelope(streamId, 1)), Version.initial());

    // Save at schema version 3
    store.saveSnapshot(streamId, result.finalVersion(), new CartState(), 3);

    // Load at version 3 — should use snapshot
    AggregateHistory match = store.load(streamId, 3);
    assertNotNull(match.snapshotState(), "Snapshot should be used when versions match");

    // Load at version 2 — should ignore snapshot
    AggregateHistory mismatch = store.load(streamId, 2);
    assertNull(mismatch.snapshotState(), "Snapshot should be ignored on version mismatch");
  }

  private EventEnvelope makeEnvelope(StreamId streamId, long version) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        new Version(version),
        new EventType("ItemAdded"),
        new CartEvent.ItemAdded("SKU-TEST", 1),
        new EventMetadata(
            EventId.of("evt-" + UUID.randomUUID()),
            CommandId.of("cmd-" + UUID.randomUUID()),
            null,
            null,
            CorrelationId.of("corr-test"),
            null,
            null,
            Instant.now()));
  }
}
