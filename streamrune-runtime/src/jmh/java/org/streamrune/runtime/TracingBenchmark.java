package org.streamrune.runtime;

import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.runtime.StreamRuneBenchmark.BenchmarkCommand;
import org.streamrune.runtime.StreamRuneBenchmark.BenchmarkDecider;
import org.streamrune.runtime.StreamRuneBenchmark.BenchmarkEvent;
import org.streamrune.runtime.StreamRuneBenchmark.BenchmarkState;
import org.streamrune.test.InMemoryEventStore;

/**
 * The cost of tracing one command: {@code VirtualThreadCommandBus.execute} on the serializing
 * {@link InMemoryEventStore}, with the bus, its default locker and the store traced through the
 * {@code tracing} parameter. {@code NOOP} traces nothing; {@code SAMPLER_OFF} is {@link
 * OpenTelemetryTracing} over an SDK whose sampler drops every trace (the spans are started but not
 * recorded); {@code SAMPLER_ON} records every span (command, lock, load, deserialize, append,
 * serialize) and hands it to a batch processor whose exporter discards it.
 */
public class TracingBenchmark {

  private static final AggregateType TYPE = AggregateType.of("benchmark");

  @State(Scope.Thread)
  public static class TracedBusState {

    @Param({"NOOP", "SAMPLER_OFF", "SAMPLER_ON"})
    public String tracing;

    public VirtualThreadCommandBus bus;
    private SdkTracerProvider provider;

    @Setup(Level.Iteration)
    public void setup() {
      StreamRuneTracing traced = StreamRuneTracing.NOOP;
      if (!tracing.equals("NOOP")) {
        provider =
            SdkTracerProvider.builder()
                .setSampler(tracing.equals("SAMPLER_ON") ? Sampler.alwaysOn() : Sampler.alwaysOff())
                .addSpanProcessor(BatchSpanProcessor.builder(new DiscardingExporter()).build())
                .build();
        traced =
            OpenTelemetryTracing.of(
                OpenTelemetrySdk.builder().setTracerProvider(provider).build(),
                TracingSettings.allEnabled());
      }
      InMemoryEventStore store =
          InMemoryEventStore.serializing(
                  SimpleEventTypeRegistry.builder()
                      .registerEvent("BenchmarkEvent", BenchmarkEvent.class)
                      .registerState("BenchmarkState", BenchmarkState.class)
                      .build(),
                  null)
              .withTracing(traced);
      bus =
          VirtualThreadCommandBus.builder()
              .eventStore(store)
              .register(TYPE, BenchmarkCommand.class, BenchmarkCommand::id, new BenchmarkDecider())
              .tracing(traced)
              .build();
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
      bus.close();
      if (provider != null) {
        provider.close();
      }
    }
  }

  /** Accepts every batch and keeps nothing. */
  static final class DiscardingExporter implements SpanExporter {
    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      return CompletableResultCode.ofSuccess();
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 2, time = 1)
  @Measurement(iterations = 3, time = 1)
  @Fork(1)
  public void commandBusThroughput(TracedBusState state) {
    state.bus.execute(new BenchmarkCommand(AggregateId.of("aggregate-" + UUID.randomUUID())));
  }
}
