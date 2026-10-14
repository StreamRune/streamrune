package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.test.SagaForwardFenceContract;
import org.streamrune.test.SagaForwardFenceContract.FenceEvent;
import org.streamrune.test.SagaForwardFenceContract.FenceState;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * What the forward fence costs a keyed one-event append on PostgreSQL: latency (p50, p99) and WAL
 * bytes per append, without and with a saga origin, in the order A, B, A, B so drift shows up as a
 * difference between the two A runs. Not part of {@code check}: run it with {@code
 * STREAMRUNE_MEASURE=true}. It accepts the cost when the median p99 with the fence exceeds the
 * median p99 without it by no more than the difference between the two runs without it plus 0.5 ms.
 */
@EnabledIfEnvironmentVariable(named = "STREAMRUNE_MEASURE", matches = "true")
class SagaForwardFenceCostMeasurement {

  private static final int WARM_UP = 500;
  private static final int MEASURED = 2000;
  private static final SagaType TYPE = SagaType.of("fence.CostSaga");
  private static final SagaId ID = SagaId.of("cost-saga-1");

  private record Run(String name, double p50Ms, double p99Ms, double walBytesPerAppend) {}

  @Test
  void measureTheFence() throws Exception {
    try (var pg =
        new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("fence_cost")) {
      pg.start();
      HikariConfig config = new HikariConfig();
      config.setJdbcUrl(pg.getJdbcUrl());
      config.setUsername(pg.getUsername());
      config.setPassword(pg.getPassword());
      config.setMaximumPoolSize(2);
      try (HikariDataSource dataSource = new HikariDataSource(config)) {
        EventTypeRegistry registry =
            new EventTypeRegistry() {
              @Override
              public Class<?> resolveEventType(EventType eventType) {
                return FenceEvent.class;
              }

              @Override
              public Class<?> resolveStateType(String stateType) {
                return Object.class;
              }

              @Override
              public Collection<Class<?>> registeredTypes() {
                return List.of();
              }
            };
        new PostgresEventStoreFactory(dataSource, registry).create();
        new PostgresSagaStore(dataSource, new ObjectMapper().registerModule(new JavaTimeModule()))
            .create(ID, TYPE, new FenceState(SagaStatus.RUNNING), SagaStatus.RUNNING);
        PostgresEventStore store =
            PostgresEventStore.builder()
                .dataSource(dataSource)
                .typeRegistry(registry)
                .commandInbox(new PostgresCommandInbox(dataSource))
                .build();

        List<Run> runs = new ArrayList<>();
        runs.add(run(dataSource, store, "A1 unfenced", null));
        runs.add(run(dataSource, store, "B1 fenced", SagaOrigin.dispatchedBy(TYPE, ID)));
        runs.add(run(dataSource, store, "A2 unfenced", null));
        runs.add(run(dataSource, store, "B2 fenced", SagaOrigin.dispatchedBy(TYPE, ID)));

        StringBuilder table =
            new StringBuilder(
                "| run | p50 ms | p99 ms | WAL bytes / append |\n|---|---|---|---|\n");
        for (Run r : runs) {
          table.append(
              String.format(
                  "| %s | %.3f | %.3f | %.0f |%n",
                  r.name(), r.p50Ms(), r.p99Ms(), r.walBytesPerAppend()));
        }
        double medianA = (runs.get(0).p99Ms() + runs.get(2).p99Ms()) / 2;
        double medianB = (runs.get(1).p99Ms() + runs.get(3).p99Ms()) / 2;
        double noise = Math.abs(runs.get(0).p99Ms() - runs.get(2).p99Ms());
        double allowed = noise + 0.5;
        table.append(
            String.format(
                "%nmedian p99 unfenced %.3f ms, fenced %.3f ms, difference %.3f ms;"
                    + " allowed %.3f ms (|A1 - A2| = %.3f ms + 0.5 ms)%n",
                medianA, medianB, medianB - medianA, allowed, noise));
        System.out.println(table);
        assertTrue(
            medianB - medianA <= allowed,
            "the fence's p99 cost is outside the append's noise:\n" + table);
      }
    }
  }

  private static Run run(
      HikariDataSource dataSource, PostgresEventStore store, String name, SagaOrigin forwardOf)
      throws SQLException {
    String prefix = name.substring(0, 2).toLowerCase();
    for (int i = 0; i < WARM_UP; i++) {
      append(store, prefix + "w" + i, forwardOf);
    }
    String start = walLsn(dataSource);
    long[] nanos = new long[MEASURED];
    for (int i = 0; i < MEASURED; i++) {
      long t0 = System.nanoTime();
      append(store, prefix + "m" + i, forwardOf);
      nanos[i] = System.nanoTime() - t0;
    }
    double walBytes = walBytesSince(dataSource, start) / (double) MEASURED;
    Arrays.sort(nanos);
    return new Run(
        name,
        nanos[MEASURED / 2] / 1_000_000.0,
        nanos[(int) Math.ceil(MEASURED * 0.99) - 1] / 1_000_000.0,
        walBytes);
  }

  private static void append(PostgresEventStore store, String id, SagaOrigin forwardOf) {
    StreamId stream = StreamId.of(AggregateType.of("cost_target"), AggregateId.of(id));
    store.appendWithKey(
        stream,
        List.of(
            EventStoreFixture.event(
                stream,
                new Version(1),
                SagaForwardFenceContract.EVENT_TYPE,
                new FenceEvent("payload"))),
        Version.initial(),
        IdempotencyKey.of("cost:" + id),
        "CostCommand",
        forwardOf);
  }

  private static String walLsn(HikariDataSource dataSource) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT pg_current_wal_lsn()::text");
        ResultSet rs = ps.executeQuery()) {
      rs.next();
      return rs.getString(1);
    }
  }

  private static long walBytesSince(HikariDataSource dataSource, String start) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), ?::pg_lsn)")) {
      ps.setString(1, start);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }
}
