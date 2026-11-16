package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.postgres.PostgresEventStoreBenchmark.AppendState;
import org.streamrune.postgres.PostgresEventStoreBenchmark.LoadState;
import org.streamrune.postgres.PostgresEventStoreBenchmark.OrderRecordedV2;
import org.streamrune.postgres.PostgresEventStoreBenchmark.OrderRecordedV3;
import org.streamrune.postgres.PostgresEventStoreBenchmark.UpcastLoadState;

/**
 * Runs every {@link PostgresEventStoreBenchmark} method twice against a real {@code @Setup} state,
 * outside the JMH harness, on a Testcontainers PostgreSQL (or the server {@link BenchmarkDatabase}
 * is pointed at). Part of {@code check} through {@code jmhSmokeTest}: a benchmark that compiles but
 * throws, or measures an empty stream, fails the build. Twice, because the second call is the one
 * that meets the state the first left behind.
 */
class PostgresEventStoreBenchmarkSmokeTest {

  private final PostgresEventStoreBenchmark benchmark = new PostgresEventStoreBenchmark();

  @Test
  @DisplayName("appendToNewStream appends a whole batch to a new stream on every call")
  void appendToNewStreamRunsTwice() {
    var state = new AppendState();
    state.batchSize = 10;
    state.openDatabase();
    try {
      state.startIteration();

      assertThat(benchmark.appendToNewStream(state).finalVersion().value()).isEqualTo(10);
      assertThat(benchmark.appendToNewStream(state).finalVersion().value()).isEqualTo(10);
    } finally {
      state.closeDatabase();
    }
  }

  @Test
  @DisplayName("appendToExistingStream appends at the stream's head on every call")
  void appendToExistingStreamRunsTwice() {
    var state = new AppendState();
    state.batchSize = 10;
    state.openDatabase();
    try {
      state.startIteration();

      benchmark.appendToExistingStream(state);
      benchmark.appendToExistingStream(state);

      assertThat(state.existingHead).isEqualTo(AppendState.EXISTING_EVENTS + 20L);
      assertThat(state.store.load(state.existingStream).events())
          .hasSize(AppendState.EXISTING_EVENTS + 20);
    } finally {
      state.closeDatabase();
    }
  }

  @Test
  @DisplayName("a new iteration starts on an empty event stream")
  void iterationStartsOnAnEmptyEventStream() {
    var state = new AppendState();
    state.batchSize = 1;
    state.openDatabase();
    try {
      state.startIteration();
      benchmark.appendToNewStream(state);
      benchmark.appendToExistingStream(state);

      state.startIteration();

      assertThat(
              state.store.readGlobalStream(org.streamrune.core.types.GlobalOffset.initial(), 100))
          .hasSize(AppendState.EXISTING_EVENTS);
    } finally {
      state.closeDatabase();
    }
  }

  @Test
  @DisplayName("load reads the whole stream on every call")
  void loadRunsTwice() {
    var state = new LoadState();
    state.streamLength = 100;
    state.writeStream();
    try {
      assertThat(benchmark.load(state).events()).hasSize(100);
      assertThat(benchmark.load(state).events()).hasSize(100);
    } finally {
      state.closeDatabase();
    }
  }

  @Test
  @DisplayName("loadWithUpcasting runs one step and returns the version 2 events")
  void loadWithOneUpcastStepRunsTwice() {
    var state = new UpcastLoadState();
    state.upcastSteps = 1;
    state.writeStream();
    try {
      benchmark.loadWithUpcasting(state);
      var events = benchmark.loadWithUpcasting(state).events();

      assertThat(events).hasSize(UpcastLoadState.STREAM_LENGTH);
      assertThat(events.getLast().event())
          .isEqualTo(new OrderRecordedV2("order-99", "customer-99", 99));
    } finally {
      state.closeDatabase();
    }
  }

  @Test
  @DisplayName("loadWithUpcasting runs two steps and returns the version 3 events")
  void loadWithTwoUpcastStepsRunsTwice() {
    var state = new UpcastLoadState();
    state.upcastSteps = 2;
    state.writeStream();
    try {
      benchmark.loadWithUpcasting(state);
      var events = benchmark.loadWithUpcasting(state).events();

      assertThat(events).hasSize(UpcastLoadState.STREAM_LENGTH);
      assertThat(events)
          .extracting(EventEnvelope::event)
          .allSatisfy(event -> assertThat(event).isInstanceOf(OrderRecordedV3.class));
      assertThat(events.getFirst().event())
          .isEqualTo(new OrderRecordedV3("order-0", "customer-0", 0, "EUR"));
    } finally {
      state.closeDatabase();
    }
  }
}
