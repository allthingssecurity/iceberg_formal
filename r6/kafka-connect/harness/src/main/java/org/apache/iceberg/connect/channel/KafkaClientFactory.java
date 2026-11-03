package org.apache.iceberg.connect.channel;

import harness.FakeKafka;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;

/**
 * HARNESS SHADOW of the released org.apache.iceberg.connect.channel.KafkaClientFactory (same FQN,
 * same package-private API, identical in 1.8.1 .. 1.12.0). It is first on the classpath, so the
 * released Channel/Worker/Coordinator/CommitterImpl get in-JVM fake clients instead of real
 * KafkaProducer/KafkaConsumer/Admin. The released factory sets auto.offset.reset=latest via
 * putIfAbsent, read_committed, enable.auto.commit=false, and calls initTransactions(); the fake
 * mirrors exactly that.
 */
class KafkaClientFactory {
  public static final String SHADOW_MARKER = "r6-kafka-connect-harness";
  private final Map<String, String> kafkaProps;

  KafkaClientFactory(Map<String, String> kafkaProps) {
    this.kafkaProps = kafkaProps;
  }

  Producer<String, byte[]> createProducer(String transactionalId) {
    return FakeKafka.K.newProducer(transactionalId);
  }

  Consumer<String, byte[]> createConsumer(String consumerGroupId) {
    String reset = kafkaProps.getOrDefault(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    return FakeKafka.K.newConsumer(consumerGroupId, FakeKafka.Reset.of(reset));
  }

  Admin createAdmin() {
    return FakeKafka.K.newAdmin();
  }
}
