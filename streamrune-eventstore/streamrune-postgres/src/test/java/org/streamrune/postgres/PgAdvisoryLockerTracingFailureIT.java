package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.streamrune.core.LockException;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanBaggage;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The traced {@link PgAdvisoryLocker} never leaks the advisory lock it acquired, or the pool
 * connection that holds it, when its tracing throws: after many more failing acquisitions than the
 * lock pool has connections, no advisory lock is held in PostgreSQL, another replica can lock the
 * same streams, and the pool still serves.
 */
@Testcontainers
class PgAdvisoryLockerTracingFailureIT {

  private static final String DATABASE = "lock_tracing_failure_it";

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName(DATABASE);

  private static final AggregateType ORDER = AggregateType.of("order");

  /** The scope operation that throws. */
  enum Fault {
    START,
    SET_ATTRIBUTE,
    RECORD_SUCCESS,
    RECORD_ERROR,
    CLOSE
  }

  /** A tracing of the lock area whose {@code fault} throws what {@code failure} supplies. */
  static final class BrokenTracing implements StreamRuneTracing {
    private final Fault fault;
    private final Supplier<? extends Throwable> failure;
    final AtomicInteger thrown = new AtomicInteger();

    BrokenTracing(Fault fault, Supplier<? extends Throwable> failure) {
      this.fault = fault;
      this.failure = failure;
    }

    private void maybeThrow(Fault at) {
      if (fault == at) {
        thrown.incrementAndGet();
        sneakyThrow(failure.get());
      }
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
      throw (T) t;
    }

    @Override
    public boolean enabled(TracingArea area) {
      return area == TracingArea.LOCK;
    }

    @Override
    public TraceScope start(
        TracingArea area,
        String spanName,
        SpanKind kind,
        SpanAttributes attributes,
        SpanLink link,
        SpanBaggage baggage) {
      maybeThrow(Fault.START);
      return new TraceScope() {
        @Override
        public void setAttribute(String key, String value) {
          maybeThrow(Fault.SET_ATTRIBUTE);
        }

        @Override
        public void setAttribute(String key, long value) {
          maybeThrow(Fault.SET_ATTRIBUTE);
        }

        @Override
        public void setAttribute(String key, boolean value) {
          maybeThrow(Fault.SET_ATTRIBUTE);
        }

        @Override
        public void recordSuccess() {
          maybeThrow(Fault.RECORD_SUCCESS);
        }

        @Override
        public void recordError(Throwable error) {
          maybeThrow(Fault.RECORD_ERROR);
        }

        @Override
        public void close() {
          maybeThrow(Fault.CLOSE);
        }
      };
    }

    @Override
    public String toString() {
      return "BrokenTracing{" + fault + "}";
    }
  }

  static Stream<BrokenTracing> brokenTracings() {
    Supplier<Throwable> exception = () -> new IllegalStateException("the tracing failed");
    return Stream.of(
        new BrokenTracing(Fault.START, exception),
        new BrokenTracing(Fault.SET_ATTRIBUTE, exception),
        new BrokenTracing(Fault.RECORD_SUCCESS, exception),
        new BrokenTracing(Fault.CLOSE, exception),
        new BrokenTracing(Fault.CLOSE, () -> new OutOfMemoryError("in the span's end")));
  }

  private static int count(String sql) throws Exception {
    try (Connection conn =
            DriverManager.getConnection(PG.getJdbcUrl(), PG.getUsername(), PG.getPassword());
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      rs.next();
      return rs.getInt(1);
    }
  }

  private static int advisoryLocksHeld() throws Exception {
    return count("SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted");
  }

  private static int sessionsIdleInTransaction() throws Exception {
    return count(
        "SELECT count(*) FROM pg_stat_activity WHERE datname = '"
            + DATABASE
            + "' AND state = 'idle in transaction'");
  }

  private static StreamId order(int i) {
    return StreamId.of(ORDER, AggregateId.of("o-" + i));
  }

  @ParameterizedTest
  @MethodSource("brokenTracings")
  void noLockAndNoPoolConnectionIsLeaked_andAnotherReplicaCanLockTheSameStreams(
      BrokenTracing broken) throws Exception {
    try (PgAdvisoryLocker replicaA =
            PgAdvisoryLocker.withDedicatedPool(
                    PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(), 2)
                .traced(broken);
        PgAdvisoryLocker replicaB =
            PgAdvisoryLocker.withDedicatedPool(
                PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(), 2)) {

      // Five times the pool: a leak of one connection per failure would exhaust it twice over.
      for (int i = 0; i < 10; i++) {
        try (AutoCloseable lock = replicaA.acquireLock(order(i % 3), Duration.ofMillis(500))) {
          assertThat(lock).as("acquisition %s: the caller gets the lock", i).isNotNull();
          assertThat(advisoryLocksHeld()).as("held while the caller has it").isEqualTo(1);
        }
        assertThat(advisoryLocksHeld()).as("released once the caller closed it").isZero();
      }
      assertThat(broken.thrown.get()).as("the tracing did throw").isPositive();
      assertThat(sessionsIdleInTransaction()).isZero();

      for (int i = 0; i < 3; i++) {
        try (AutoCloseable lock = replicaB.acquireLock(order(i), Duration.ofSeconds(1))) {
          assertThat(lock).as("the other replica locks o-%s", i).isNotNull();
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("brokenTracings")
  void aLockAnotherReplicaHoldsIsStillATimeout_andLeaksNothing(BrokenTracing broken)
      throws Exception {
    try (PgAdvisoryLocker replicaA =
            PgAdvisoryLocker.withDedicatedPool(
                    PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(), 2)
                .traced(broken);
        PgAdvisoryLocker replicaB =
            PgAdvisoryLocker.withDedicatedPool(
                PG.getJdbcUrl(), PG.getUsername(), PG.getPassword(), 2)) {
      try (AutoCloseable held = replicaB.acquireLock(order(1), Duration.ofSeconds(1))) {
        for (int i = 0; i < 6; i++) {
          Throwable refused = null;
          try (AutoCloseable lock = replicaA.acquireLock(order(1), Duration.ZERO)) {
            assertThat(lock).isNull();
          } catch (Throwable t) {
            refused = t;
          }
          assertThat(refused)
              .as("the true outcome: the lock is held by another session")
              .isInstanceOf(LockException.class)
              .hasMessageContaining("held by another session");
        }
        assertThat(advisoryLocksHeld()).as("only the holder's lock").isEqualTo(1);
      }
      // The pool of replica A lost nothing to the six refusals.
      try (AutoCloseable lock = replicaA.acquireLock(order(1), Duration.ofSeconds(1))) {
        assertThat(lock).isNotNull();
      }
      assertThat(advisoryLocksHeld()).isZero();
      assertThat(sessionsIdleInTransaction()).isZero();
    }
  }
}
