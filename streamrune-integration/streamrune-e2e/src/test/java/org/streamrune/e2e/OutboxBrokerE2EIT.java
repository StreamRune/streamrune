package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.kafka.KafkaOutboxPublisher;
import org.streamrune.postgres.PostgresOutboxStore;
import org.streamrune.rabbitmq.RabbitMqOutboxPublisher;
import org.streamrune.runtime.OutboxFailedReplayer;
import org.streamrune.runtime.OutboxPoller;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end relay → broker delivery: events written to the real Postgres outbox are claimed by a
 * real {@link OutboxPoller} background loop and delivered to a real broker (Apache Kafka, RabbitMQ)
 * through the production {@link KafkaOutboxPublisher} / {@link RabbitMqOutboxPublisher}; a plain
 * broker consumer then receives them. The whole transactional-outbox → relay → broker path is
 * exercised with nothing stubbed.
 *
 * <p><b>Why these tests are isolated.</b> Booting a Kafka or RabbitMQ container is materially
 * slower and flakier than the Postgres container the rest of the e2e suite shares — under parallel
 * container startup they intermittently time out. They are tagged {@code @Tag("broker")} and the
 * module's default {@code test} task EXCLUDES that tag, so {@code :streamrune-integration:
 * streamrune-e2e:test} stays Postgres-only and reliable. Run them via the module's {@code
 * brokerTest} task (see build.gradle.kts); it is deliberately NOT wired into {@code check}.
 *
 * <p><b>Where this actually runs.</b> Not being in {@code check} once meant not running at all: no
 * workflow invoked {@code brokerTest}, so the only end-to-end per-aggregate ordering assertion in
 * the repo had never executed in CI, while every {@code OutboxPollerTest} uses a fake publisher and
 * every outbox-module broker integration test calls {@code publisher.publish(entry)} directly —
 * bypassing the poller, the claim lease and ordering. {@code .github/workflows/broker-e2e-test.yml}
 * now runs it nightly, on {@code workflow_dispatch}, and on every {@code v*} tag push, so a release
 * cut cannot ship without it.
 *
 * <p>Each container is started lazily inside its own test and stopped in a finally block, so the
 * Postgres-only default run never even constructs a broker container. Postgres comes from the
 * shared singleton in {@link E2ETestBase} (its {@code @BeforeAll} starts it and the outbox schema).
 *
 * <p><b>Non-vacuous ordering assertion.</b> Each test saves three outbox entries for ONE aggregate
 * id, oldest-first. Because all three share one aggregate, strict per-aggregate ordering means a
 * single {@code loadPending} poll claims only that aggregate's head (its lowest-{@code seq} entry);
 * the relay poller loops, claiming seq 1, then 2, then 3 as each predecessor is delivered. Kafka
 * keys every record by the aggregate id (one partition for one key) while RabbitMQ delivers a
 * single queue in FIFO order — so the consumer must observe seq 1, 2, 3 in that exact order. A
 * relay that lost ordering, dropped an entry, or duplicated one would fail the {@code
 * containsExactly} assertion.
 */
@Tag("broker")
@Timeout(180)
class OutboxBrokerE2EIT extends E2ETestBase {

  /** One aggregate => one Kafka partition / one ordered RabbitMQ stream. */
  private static final String AGGREGATE_ID = "order-ordering-1";

  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("order"), AggregateId.of(AGGREGATE_ID));

  // E2ETestBase#cleanDatabase (@BeforeEach) already truncates outbox_events before each test, so
  // each broker test starts from an empty outbox sharing the singleton Postgres container.

  /**
   * Saves three PENDING entries for {@link #AGGREGATE_ID}, oldest first, each payload tagged with
   * its sequence number so the consumer side can assert ordering. Returns the entry ids in order.
   */
  private List<String> seedOrderedEntries(PostgresOutboxStore store) {
    var ids = new ArrayList<String>();
    for (int seq = 1; seq <= 3; seq++) {
      var id = OutboxEntryId.of("ord-" + seq + "-" + UUID.randomUUID());
      store.save(OutboxEntry.pending(id, "{\"seq\":" + seq + "}", "OrderEvent", STREAM));
      ids.add(id.value());
    }
    return ids;
  }

  // ---- Kafka ---------------------------------------------------------------------------------

  @Test
  void kafkaRelayDeliversOutboxEntriesInAggregateOrder() throws Exception {
    try (var kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"))) {
      kafka.start();
      var topic = "outbox-e2e-" + UUID.randomUUID();
      var producer =
          new KafkaProducer<String, String>(
              Map.of(
                  ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                  ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                  ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));
      try (var consumer = newKafkaConsumer(kafka)) {
        consumer.subscribe(List.of(topic));

        var store = new PostgresOutboxStore(dataSource, Duration.ofMinutes(5));
        List<String> savedIds = seedOrderedEntries(store);

        // Real publisher, real relay loop: loadPending (claim) -> publish -> markDelivered.
        var publisher = KafkaOutboxPublisher.builder().producer(producer).topic(topic).build();
        var poller =
            OutboxPoller.builder()
                .outboxStore(store)
                .publisher(publisher)
                .batchSize(10)
                .pollInterval(Duration.ofMillis(100))
                .build();

        poller.start();
        try {
          var records = consumeKafka(consumer, 3);

          // At-least-once delivery with per-aggregate ordering: all three keyed by the aggregate id
          // (=> one partition => publish order preserved), payloads in seq order, all entry ids
          // seen.
          assertThat(records).extracting(ConsumerRecord::key).containsOnly(STREAM.value());
          // Postgres stores the outbox payload as JSONB and re-serializes it with a space after the
          // colon, so the bytes the relay reads back and ships are "{\"seq\": N}" (not the compact
          // form saved). The ordering — seq 1, 2, 3 — is the load-bearing assertion.
          assertThat(records)
              .extracting(ConsumerRecord::value)
              .containsExactly("{\"seq\": 1}", "{\"seq\": 2}", "{\"seq\": 3}");
          assertThat(records)
              .extracting(r -> headerValue(r, "X-Outbox-Entry-Id"))
              .containsExactlyElementsOf(savedIds);
          assertThat(records)
              .extracting(r -> headerValue(r, "X-Outbox-Payload-Type"))
              .containsOnly("OrderEvent");

          // Await DELIVERED while the relay is STILL RUNNING: the consumer observes a record the
          // instant publish() returns, but markDelivered() commits just after — closing the poller
          // here could interrupt the loop mid-cycle before the last row flips to DELIVERED.
          await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertAllDelivered(savedIds));
        } finally {
          poller.close();
        }

        // Every entry reached the broker and was marked DELIVERED, so none is re-claimable.
        assertThat(new PostgresOutboxStore(dataSource, Duration.ofMinutes(5)).loadPending(10))
            .as("delivered entries are not re-claimable")
            .isEmpty();
      } finally {
        producer.close();
      }
    }
  }

  private KafkaConsumer<String, String> newKafkaConsumer(KafkaContainer kafka) {
    return new KafkaConsumer<>(
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, "e2e-group-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));
  }

  private static List<ConsumerRecord<String, String>> consumeKafka(
      KafkaConsumer<String, String> consumer, int expected) {
    var collected = new ArrayList<ConsumerRecord<String, String>>();
    long deadline = System.currentTimeMillis() + 60_000;
    while (collected.size() < expected && System.currentTimeMillis() < deadline) {
      consumer.poll(Duration.ofSeconds(1)).forEach(collected::add);
    }
    assertThat(collected).as("all entries delivered to Kafka").hasSize(expected);
    return collected;
  }

  private static String headerValue(ConsumerRecord<String, String> record, String key) {
    var header = record.headers().lastHeader(key);
    return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
  }

  // ---- RabbitMQ ------------------------------------------------------------------------------

  @Test
  void rabbitRelayDeliversOutboxEntriesInAggregateOrder() throws Exception {
    String exchange = "outbox-e2e";
    String queue = "outbox-e2e-queue";
    String routingKey = "e2e";

    try (var rabbit = new RabbitMQContainer("rabbitmq:4-management-alpine")) {
      rabbit.start();
      var factory = new ConnectionFactory();
      factory.setHost(rabbit.getHost());
      factory.setPort(rabbit.getAmqpPort());
      factory.setUsername(rabbit.getAdminUsername());
      factory.setPassword(rabbit.getAdminPassword());

      try (Connection connection = factory.newConnection();
          Channel consumeChannel = connection.createChannel()) {
        // build() enables publisher confirms on the publish channel.
        Channel publishChannel = connection.createChannel();
        consumeChannel.exchangeDeclare(exchange, "direct", true);
        consumeChannel.queueDeclare(queue, true, false, false, null);
        consumeChannel.queueBind(queue, exchange, routingKey);

        var store = new PostgresOutboxStore(dataSource, Duration.ofMinutes(5));
        List<String> savedIds = seedOrderedEntries(store);

        // Real publisher (persistent delivery + publisher confirms), real relay loop.
        var publisher =
            RabbitMqOutboxPublisher.builder()
                .channel(publishChannel)
                .exchange(exchange)
                .routingKey(routingKey)
                .build();
        var poller =
            OutboxPoller.builder()
                .outboxStore(store)
                .publisher(publisher)
                .batchSize(10)
                .pollInterval(Duration.ofMillis(100))
                .build();

        poller.start();
        var bodies = new ArrayList<String>();
        var entryIds = new ArrayList<String>();
        try {
          await()
              .atMost(Duration.ofSeconds(60))
              .untilAsserted(
                  () -> {
                    GetResponse response = consumeChannel.basicGet(queue, true);
                    if (response != null) {
                      bodies.add(new String(response.getBody(), StandardCharsets.UTF_8));
                      entryIds.add(
                          response.getProps().getHeaders().get("X-Outbox-Entry-Id").toString());
                      // Persistent delivery mode survives a broker restart (delivery mode 2).
                      assertThat(response.getProps().getDeliveryMode()).isEqualTo(2);
                    }
                    assertThat(bodies).hasSize(3);
                  });

          // Await DELIVERED while the relay is still running (same publish-then-mark race as the
          // Kafka test): markDelivered commits just after the message is on the broker.
          await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertAllDelivered(savedIds));
        } finally {
          poller.close();
          publishChannel.close();
        }

        // FIFO queue => publish order == consume order == aggregate (createdAt) order. Payloads are
        // the JSONB-normalized "{\"seq\": N}" the relay read back from Postgres (see the Kafka
        // test).
        assertThat(bodies).containsExactly("{\"seq\": 1}", "{\"seq\": 2}", "{\"seq\": 3}");
        assertThat(entryIds).containsExactlyElementsOf(savedIds);
        assertThat(new PostgresOutboxStore(dataSource, Duration.ofMinutes(5)).loadPending(10))
            .as("delivered entries are not re-claimable")
            .isEmpty();
      }
    }
  }

  // ---- Strict channel: a FAILED head blocks its aggregate at a real broker -----------------

  @Test
  void rabbitRelay_strict_failedHeadBlocksAggregate_skipReleases() throws Exception {
    // n0 is unroutable (no queue bound yet) → basic.return → UnroutableException → ENTRY → the
    // 2-attempt ladder terminal-FAILs it. n1/n2 of the same aggregate must NOT reach the broker
    // while n0 is FAILED. Binding the queue and skipping n0 releases them, in order; n0 never
    // arrives (a skip is not a delivery).
    String exchange = "outbox-e2e-strict";
    String queue = "outbox-e2e-strict-queue";
    String routingKey = "e2e-strict";
    try (var rabbit = new RabbitMQContainer("rabbitmq:4-management-alpine")) {
      rabbit.start();
      var factory = new ConnectionFactory();
      factory.setHost(rabbit.getHost());
      factory.setPort(rabbit.getAmqpPort());
      factory.setUsername(rabbit.getAdminUsername());
      factory.setPassword(rabbit.getAdminPassword());
      try (Connection connection = factory.newConnection();
          Channel consumeChannel = connection.createChannel()) {
        Channel publishChannel = connection.createChannel();
        consumeChannel.exchangeDeclare(exchange, "direct", true);
        consumeChannel.queueDeclare(queue, true, false, false, null);
        // Deliberately NOT bound yet: every publish is returned as unroutable.

        var store =
            new PostgresOutboxStore(
                dataSource, Duration.ofMinutes(5), OutboxOrderingMode.STRICT_PER_AGGREGATE);
        List<String> savedIds = seedOrderedEntries(store); // seq 1, 2, 3 of AGGREGATE_ID
        var publisher =
            RabbitMqOutboxPublisher.builder()
                .channel(publishChannel)
                .exchange(exchange)
                .routingKey(routingKey)
                .build();
        var poller =
            OutboxPoller.builder()
                .outboxStore(store)
                .publisher(publisher)
                .retryPolicy(new RetryPolicy(2, Duration.ofMillis(50), 2.0, false))
                .batchSize(10)
                .pollInterval(Duration.ofMillis(100))
                .build();
        poller.start();
        try {
          await()
              .atMost(Duration.ofSeconds(30))
              .untilAsserted(() -> assertThat(statusOf(savedIds.get(0))).isEqualTo("FAILED"));
          // Hold for a few polls: the successors stay PENDING and nothing reaches the broker.
          Thread.sleep(1_500);
          assertThat(statusOf(savedIds.get(1))).isEqualTo("PENDING");
          assertThat(statusOf(savedIds.get(2))).isEqualTo("PENDING");
          assertThat(store.sampleBlockage().failedAggregates()).isEqualTo(1);

          consumeChannel.queueBind(queue, exchange, routingKey); // the "fix"
          var outcome =
              new OutboxFailedReplayer(store, StreamRuneMetrics.NOOP)
                  .skip(
                      OutboxEntryId.of(savedIds.get(0)),
                      "e2e-operator",
                      "unroutable while unbound");
          assertThat(outcome).isEqualTo(OutboxFailedReplayer.SkipOutcome.SKIPPED);

          var bodies = new ArrayList<String>();
          await()
              .atMost(Duration.ofSeconds(60))
              .untilAsserted(
                  () -> {
                    GetResponse response = consumeChannel.basicGet(queue, true);
                    if (response != null) {
                      bodies.add(new String(response.getBody(), StandardCharsets.UTF_8));
                    }
                    assertThat(bodies).hasSize(2);
                  });
          assertThat(bodies).containsExactly("{\"seq\": 2}", "{\"seq\": 3}");
          await()
              .atMost(Duration.ofSeconds(20))
              .untilAsserted(() -> assertAllDelivered(savedIds.subList(1, 3)));
          assertThat(statusOf(savedIds.get(0))).isEqualTo("SKIPPED");
          Thread.sleep(500);
          assertThat(consumeChannel.basicGet(queue, true)).as("n0 is never delivered").isNull();
        } finally {
          poller.close();
          publishChannel.close();
        }
      }
    }
  }

  @Test
  void kafkaRelay_strict_replayDeliversHeadBeforeSuccessors() throws Exception {
    // The topic rejects records above 1 KiB (max.message.bytes) → RecordTooLargeException from the
    // broker: non-retriable → ENTRY → FAILED. n1/n2 wait. Raising the topic limit and replaying n0
    // delivers n0, then n1, then n2 — the replayed head goes first.
    try (var kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"))) {
      kafka.start();
      var topic = "outbox-e2e-strict-" + UUID.randomUUID();
      try (var admin =
          org.apache.kafka.clients.admin.AdminClient.create(
              Map.of(
                  org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                  kafka.getBootstrapServers()))) {
        admin
            .createTopics(
                List.of(
                    new org.apache.kafka.clients.admin.NewTopic(topic, 1, (short) 1)
                        .configs(Map.of("max.message.bytes", "1024"))))
            .all()
            .get(30, java.util.concurrent.TimeUnit.SECONDS);

        var producer =
            new KafkaProducer<String, String>(
                Map.of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        StringSerializer.class.getName()));
        try (var consumer = newKafkaConsumer(kafka)) {
          consumer.subscribe(List.of(topic));
          var store =
              new PostgresOutboxStore(
                  dataSource, Duration.ofMinutes(5), OutboxOrderingMode.STRICT_PER_AGGREGATE);
          // n0 is 2 KiB — over the topic limit; n1 and n2 are small.
          var ids = new ArrayList<String>();
          var big = OutboxEntryId.of("big-1-" + UUID.randomUUID());
          store.save(
              OutboxEntry.pending(
                  big, "{\"seq\":1,\"pad\":\"" + "x".repeat(2048) + "\"}", "OrderEvent", STREAM));
          ids.add(big.value());
          for (int seq = 2; seq <= 3; seq++) {
            var id = OutboxEntryId.of("ord-" + seq + "-" + UUID.randomUUID());
            store.save(OutboxEntry.pending(id, "{\"seq\":" + seq + "}", "OrderEvent", STREAM));
            ids.add(id.value());
          }
          var publisher = KafkaOutboxPublisher.builder().producer(producer).topic(topic).build();
          var poller =
              OutboxPoller.builder()
                  .outboxStore(store)
                  .publisher(publisher)
                  .retryPolicy(new RetryPolicy(2, Duration.ofMillis(50), 2.0, false))
                  .batchSize(10)
                  .pollInterval(Duration.ofMillis(100))
                  .build();
          poller.start();
          try {
            await()
                .atMost(Duration.ofSeconds(60))
                .untilAsserted(() -> assertThat(statusOf(ids.get(0))).isEqualTo("FAILED"));
            Thread.sleep(1_500);
            assertThat(statusOf(ids.get(1))).isEqualTo("PENDING");
            assertThat(consumer.poll(Duration.ofMillis(500)).count())
                .as("nothing delivered while blocked")
                .isZero();

            // The fix: raise the topic limit; then replay the head.
            var resource =
                new org.apache.kafka.common.config.ConfigResource(
                    org.apache.kafka.common.config.ConfigResource.Type.TOPIC, topic);
            admin
                .incrementalAlterConfigs(
                    Map.of(
                        resource,
                        List.of(
                            new org.apache.kafka.clients.admin.AlterConfigOp(
                                new org.apache.kafka.clients.admin.ConfigEntry(
                                    "max.message.bytes", "1048588"),
                                org.apache.kafka.clients.admin.AlterConfigOp.OpType.SET))))
                .all()
                .get(30, java.util.concurrent.TimeUnit.SECONDS);
            var outcome = new OutboxFailedReplayer(store, StreamRuneMetrics.NOOP).replay(big);
            assertThat(outcome).isEqualTo(OutboxFailedReplayer.ReplayOutcome.REPLAYED);

            var records = consumeKafka(consumer, 3);
            assertThat(records)
                .extracting(r -> headerValue(r, "X-Outbox-Entry-Id"))
                .containsExactlyElementsOf(ids);
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertAllDelivered(ids));
          } finally {
            poller.close();
          }
        } finally {
          producer.close();
        }
      }
    }
  }

  // ---- Shared assertions ---------------------------------------------------------------------

  private static void assertAllDelivered(List<String> entryIds) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT status FROM outbox_events WHERE entry_id = ?")) {
      for (String id : entryIds) {
        ps.setString(1, id);
        try (var rs = ps.executeQuery()) {
          assertThat(rs.next()).as("row %s exists", id).isTrue();
          assertThat(rs.getString("status")).as("row %s delivered", id).isEqualTo("DELIVERED");
        }
      }
    }
  }

  private static String statusOf(String entryId) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT status FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, entryId);
      try (var rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        return rs.getString(1);
      }
    }
  }
}
