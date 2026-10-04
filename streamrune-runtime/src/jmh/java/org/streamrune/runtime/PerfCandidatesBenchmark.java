package org.streamrune.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.streamrune.core.IdGenerator;

/**
 * Micro-benchmarks isolating four candidate StreamRune hot-path optimizations surfaced by the
 * perf-hotspot analysis. Each candidate is a pair of @Benchmark methods — the CURRENT mechanism vs
 * the PROPOSED one — run under identical JIT/machine conditions so the delta is a clean
 * apples-to-apples comparison. These exercise the underlying JDK/Jackson mechanism (not the full
 * production object graph) so they need no database and are reproducible on a laptop.
 *
 * <p>Run: {@code ./gradlew :streamrune-runtime:jmh -Pjmh.include=PerfCandidatesBenchmark}
 */
public class PerfCandidatesBenchmark {

  // ====================================================================================
  // A. Upcasting payload re-bind: Map -> target class.
  //    Current PostgresEventStore.readEnvelope does writeValueAsString(map) + readValue(json, cls).
  //    Proposed: objectMapper.convertValue(map, cls) — one pass, no intermediate JSON string.
  // ====================================================================================

  public record SampleEvent(
      String orderId,
      String customerId,
      long amountCents,
      String currency,
      String status,
      String note,
      int quantity,
      boolean priority) {}

  @State(Scope.Thread)
  public static class UpcastState {
    final ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> upcastedMap;

    @Setup
    public void setup() {
      upcastedMap = new LinkedHashMap<>();
      upcastedMap.put("orderId", "order-12345");
      upcastedMap.put("customerId", "cust-98765");
      upcastedMap.put("amountCents", 1999L);
      upcastedMap.put("currency", "USD");
      upcastedMap.put("status", "CONFIRMED");
      upcastedMap.put("note", "expedited shipping requested");
      upcastedMap.put("quantity", 3);
      upcastedMap.put("priority", true);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 5, time = 1)
  @Fork(1)
  public SampleEvent upcast_current_writeThenRead(UpcastState s) throws Exception {
    String json = s.mapper.writeValueAsString(s.upcastedMap);
    return s.mapper.readValue(json, SampleEvent.class);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 5, time = 1)
  @Fork(1)
  public SampleEvent upcast_proposed_convertValue(UpcastState s) {
    return s.mapper.convertValue(s.upcastedMap, SampleEvent.class);
  }

  // ====================================================================================
  // B. CachedCryptoEngine cache-key hash: MessageDigest.getInstance("SHA-256") per call (incl.
  //    cache hits) vs a reused (thread-local) MessageDigest with reset().
  // ====================================================================================

  @State(Scope.Thread)
  public static class DigestState {
    byte[] ciphertext;
    MessageDigest reusable;

    @Setup
    public void setup() throws Exception {
      // Representative AES-GCM ciphertext: 12-byte IV + ~64-byte body + 16-byte tag.
      ciphertext = new byte[92];
      new SecureRandom().nextBytes(ciphertext);
      reusable = MessageDigest.getInstance("SHA-256");
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 5, time = 1)
  @Fork(1)
  public String sha256_current_getInstancePerCall(DigestState s) throws Exception {
    return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.ciphertext));
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 5, time = 1)
  @Fork(1)
  public String sha256_proposed_reuseDigest(DigestState s) {
    s.reusable.reset();
    return HexFormat.of().formatHex(s.reusable.digest(s.ciphertext));
  }

  // ====================================================================================
  // C. Crypto engine cipher: Cipher.getInstance("AES/GCM/NoPadding") per encrypt vs a reused
  //    (thread-local) Cipher. init()+doFinal() are common to both; getInstance is the delta.
  // ====================================================================================

  @State(Scope.Thread)
  public static class CipherState {
    SecretKey key;
    byte[] plaintext;
    final SecureRandom random = new SecureRandom();
    Cipher reusable;

    @Setup
    public void setup() throws Exception {
      KeyGenerator gen = KeyGenerator.getInstance("AES");
      gen.init(256);
      key = gen.generateKey();
      plaintext = "customer@example.com|+1-555-0142|742 Evergreen Terrace".getBytes();
      reusable = Cipher.getInstance("AES/GCM/NoPadding");
    }

    GCMParameterSpec freshSpec() {
      byte[] iv = new byte[12];
      random.nextBytes(iv);
      return new GCMParameterSpec(128, iv);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 5, time = 1)
  @Fork(1)
  public byte[] cipher_current_getInstancePerCall(CipherState s) throws Exception {
    Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
    cipher.init(Cipher.ENCRYPT_MODE, s.key, s.freshSpec());
    return cipher.doFinal(s.plaintext);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.NANOSECONDS)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 5, time = 1)
  @Fork(1)
  public byte[] cipher_proposed_reuseCipher(CipherState s) throws Exception {
    s.reusable.init(Cipher.ENCRYPT_MODE, s.key, s.freshSpec());
    return s.reusable.doFinal(s.plaintext);
  }

  // ====================================================================================
  // D. ID generation RNG under concurrency. IdGenerator uses ONE shared static SecureRandom;
  //    nextBytes() takes an internal lock. Compare shared SecureRandom vs thread-local SecureRandom
  //    vs ThreadLocalRandom, under 8 threads, to quantify contention. (Throughput: higher better.)
  // ====================================================================================

  static final SecureRandom SHARED_SECURE_RANDOM = new SecureRandom();

  @State(Scope.Benchmark)
  public static class RngState {
    final ThreadLocal<SecureRandom> threadLocalSecure = ThreadLocal.withInitial(SecureRandom::new);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 5, time = 1)
  @Fork(1)
  @Threads(8)
  public void rng_current_sharedSecureRandom(Blackhole bh) {
    byte[] b = new byte[8];
    SHARED_SECURE_RANDOM.nextBytes(b);
    bh.consume(b);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 5, time = 1)
  @Fork(1)
  @Threads(8)
  public void rng_proposed_threadLocalSecureRandom(RngState s, Blackhole bh) {
    byte[] b = new byte[8];
    s.threadLocalSecure.get().nextBytes(b);
    bh.consume(b);
  }

  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 5, time = 1)
  @Fork(1)
  @Threads(8)
  public void rng_alt_threadLocalRandom(Blackhole bh) {
    byte[] b = new byte[8];
    ThreadLocalRandom.current().nextBytes(b);
    bh.consume(b);
  }

  // The REAL production method: generateEventId() = timestamp prefix + base64(random 16 bytes).
  // Swapping its RNG only speeds up the random part, so the end-to-end win is smaller than the
  // isolated RNG delta above — this measures the actual per-id cost under 8 threads, before vs
  // after the IdGenerator change.
  @Benchmark
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  @Warmup(iterations = 3, time = 1)
  @Measurement(iterations = 5, time = 1)
  @Fork(1)
  @Threads(8)
  public void idGen_generateEventId(Blackhole bh) {
    bh.consume(IdGenerator.generateEventId());
  }
}
