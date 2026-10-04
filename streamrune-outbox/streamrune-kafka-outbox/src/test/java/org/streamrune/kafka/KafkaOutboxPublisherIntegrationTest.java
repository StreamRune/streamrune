package org.streamrune.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class KafkaOutboxPublisherIntegrationTest {

  @Container
  static final KafkaContainer KAFKA =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.0"));

  static KafkaProducer<String, String> producer;

  @BeforeAll
  static void init() {
    producer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));
  }

  @AfterAll
  static void cleanup() {
    if (producer != null) producer.close();
  }

  private KafkaConsumer<String, String> newConsumer(String topic) {
    var consumer =
        new KafkaConsumer<String, String>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName()));
    consumer.subscribe(List.of(topic));
    return consumer;
  }

  private ConsumerRecord<String, String> pollOne(KafkaConsumer<String, String> consumer) {
    long deadline = System.currentTimeMillis() + 10_000;
    while (System.currentTimeMillis() < deadline) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
      if (records.count() == 1) return records.iterator().next();
      if (records.count() > 1) fail("Expected 1 record, got " + records.count());
    }
    return fail("No record arrived within timeout");
  }

  @Test
  void roundtrip_publish_and_consume() throws Exception {
    var topic = "outbox-test-" + UUID.randomUUID();
    try (var consumer = newConsumer(topic)) {
      var publisher = KafkaOutboxPublisher.builder().producer(producer).topic(topic).build();
      var entry =
          OutboxEntry.pending(OutboxEntryId.of("rt-1"), "{\"order\":\"abc\"}", "OrderCreated");

      publisher.publish(entry);

      var record = pollOne(consumer);
      assertEquals("rt-1", record.key());
      assertEquals("{\"order\":\"abc\"}", record.value());
    }
  }

  @Test
  void roundtrip_keys_a_streamed_entry_by_the_streams_text_form() throws Exception {
    var topic = "outbox-test-" + UUID.randomUUID();
    try (var consumer = newConsumer(topic)) {
      var publisher = KafkaOutboxPublisher.builder().producer(producer).topic(topic).build();
      var entry =
          OutboxEntry.pending(
              OutboxEntryId.of("rt-stream-1"),
              "{}",
              "OrderCreated",
              StreamId.of(AggregateType.of("order"), AggregateId.of("o-1")));

      publisher.publish(entry);

      var record = pollOne(consumer);
      assertEquals("order:o-1", record.key());
    }
  }

  @Test
  void headers_propagated() throws Exception {
    var topic = "outbox-test-" + UUID.randomUUID();
    try (var consumer = newConsumer(topic)) {
      var publisher = KafkaOutboxPublisher.builder().producer(producer).topic(topic).build();
      var entry = OutboxEntry.pending(OutboxEntryId.of("hdr-1"), "{}", "OrderShipped");

      publisher.publish(entry);

      var record = pollOne(consumer);
      var entryIdHeader = record.headers().lastHeader("X-Outbox-Entry-Id");
      var payloadTypeHeader = record.headers().lastHeader("X-Outbox-Payload-Type");
      assertNotNull(entryIdHeader);
      assertNotNull(payloadTypeHeader);
      assertEquals("hdr-1", new String(entryIdHeader.value(), StandardCharsets.UTF_8));
      assertEquals("OrderShipped", new String(payloadTypeHeader.value(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void multiple_entries_all_delivered() throws Exception {
    var topic = "outbox-test-" + UUID.randomUUID();
    try (var consumer = newConsumer(topic)) {
      var publisher = KafkaOutboxPublisher.builder().producer(producer).topic(topic).build();
      publisher.publish(OutboxEntry.pending(OutboxEntryId.of("m1"), "{\"seq\":1}", "E"));
      publisher.publish(OutboxEntry.pending(OutboxEntryId.of("m2"), "{\"seq\":2}", "E"));
      publisher.publish(OutboxEntry.pending(OutboxEntryId.of("m3"), "{\"seq\":3}", "E"));

      var collected = new java.util.ArrayList<ConsumerRecord<String, String>>();
      long deadline = System.currentTimeMillis() + 30_000;
      while (collected.size() < 3 && System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofSeconds(2)).forEach(collected::add);
      }
      assertEquals(3, collected.size(), "Expected exactly 3 records");
      var keys = collected.stream().map(ConsumerRecord::key).toList();
      assertTrue(keys.contains("m1"), "m1 missing");
      assertTrue(keys.contains("m2"), "m2 missing");
      assertTrue(keys.contains("m3"), "m3 missing");
    }
  }
}
