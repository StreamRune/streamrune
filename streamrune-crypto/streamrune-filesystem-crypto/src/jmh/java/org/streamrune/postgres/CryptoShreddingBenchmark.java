package org.streamrune.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
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
import org.streamrune.core.DomainEvent;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.crypto.filesystem.FileSystemCryptoEngine;

/**
 * Crypto-shredding on the event serialization path: an event with {@code @Encrypted} fields
 * serialized (each field encrypted) and deserialized (each field decrypted) by the {@link
 * ObjectMapper} {@link PostgresEventStore} reads and writes every event with ({@link
 * PostgresEventStore#createObjectMapper}, which registers {@code CryptoShreddingModule} on the
 * engine), on a real AES-256-GCM engine: {@link FileSystemCryptoEngine} with its keys in a
 * temporary directory. No network, no database.
 *
 * <p>The benchmark is declared in {@code org.streamrune.postgres} because {@code
 * createObjectMapper} is package-private. JMH and {@code jmhSmokeTest} run it on the class path,
 * where a package that spans two jars is one package for the class loader; neither jar is a named
 * module.
 *
 * <p>The engine is used bare, as the event store calls it, without the decrypt cache an integration
 * can put in front of it ({@code CachedCryptoEngine}): a cached decryption measures the cache, not
 * the cipher. Every encryption therefore takes a fresh cipher, encrypts, and checks the result with
 * a second cipher before it returns it; every decryption takes a fresh cipher. All fields of an
 * event belong to one subject, whose key file the first serialization of the trial creates; each
 * operation reads it again, from the operating system's page cache.
 *
 * <p>{@code encryptedFields} is the number of {@code @Encrypted} fields of the event: 1 for {@link
 * CustomerRegistered}, 5 for {@link CustomerProfileChanged}. The annotations give a local run its
 * settings; the nightly run overrides mode, unit and iterations with {@code -PbenchCi} (root
 * build). {@code CryptoShreddingBenchmarkSmokeTest} runs every method twice outside the harness, as
 * part of {@code check}.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
public class CryptoShreddingBenchmark {

  /** An event with one encrypted field. */
  public record CustomerRegistered(
      String customerId, @Encrypted(subjectId = "customerId") String email, String plan)
      implements DomainEvent {}

  /** An event with five encrypted fields. */
  public record CustomerProfileChanged(
      String customerId,
      @Encrypted(subjectId = "customerId") String email,
      @Encrypted(subjectId = "customerId") String fullName,
      @Encrypted(subjectId = "customerId") String phone,
      @Encrypted(subjectId = "customerId") String street,
      @Encrypted(subjectId = "customerId") String city,
      String plan)
      implements DomainEvent {}

  @State(Scope.Benchmark)
  public static class CryptoState {

    @Param({"1", "5"})
    public int encryptedFields;

    Path keyDirectory;
    ObjectMapper mapper;
    DomainEvent event;
    Class<? extends DomainEvent> eventType;

    /** {@link #event} as serialized: its fields encrypted, the input of the decrypt benchmark. */
    String json;

    @Setup(Level.Trial)
    public void createEngine() throws IOException {
      keyDirectory = Files.createTempDirectory("streamrune-crypto-bench");
      var engine = FileSystemCryptoEngine.builder().keyDirectory(keyDirectory).build();
      mapper = PostgresEventStore.createObjectMapper(engine, StreamRuneMetrics.NOOP);
      event =
          switch (encryptedFields) {
            case 1 -> new CustomerRegistered("customer-42", "jane.doe@example.com", "standard");
            case 5 ->
                new CustomerProfileChanged(
                    "customer-42",
                    "jane.doe@example.com",
                    "Jane Doe",
                    "+44 20 7946 0958",
                    "221B Baker Street",
                    "London",
                    "standard");
            default ->
                throw new IllegalArgumentException("No event with " + encryptedFields + " fields");
          };
      eventType = event.getClass();
      json = mapper.writeValueAsString(event);
    }

    @TearDown(Level.Trial)
    public void deleteKeys() throws IOException {
      try (Stream<Path> paths = Files.walk(keyDirectory)) {
        paths
            .sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                });
      }
    }
  }

  /** Serializes the event, encrypting each {@code @Encrypted} field. */
  @Benchmark
  public String serializeEncrypting(CryptoState state) throws IOException {
    return state.mapper.writeValueAsString(state.event);
  }

  /** Deserializes the serialized event, decrypting each {@code @Encrypted} field. */
  @Benchmark
  public DomainEvent deserializeDecrypting(CryptoState state) throws IOException {
    return state.mapper.readValue(state.json, state.eventType);
  }
}
