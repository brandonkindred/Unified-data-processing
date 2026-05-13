package com.unifieddataprocessing.pubsub.kafka;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/** Configuration for the Kafka-backed {@link com.unifieddataprocessing.pubsub.PubSubConsumer}. */
public final class KafkaConsumerConfig {

  private final String bootstrapServers;
  private final String groupId;
  private final Map<String, Object> extras;
  private final int maxInFlightMessages;

  public KafkaConsumerConfig(String bootstrapServers, String groupId) {
    this(bootstrapServers, groupId, Collections.emptyMap(), 0);
  }

  /** Creates a config with extra Kafka client properties merged in (extras may be null). */
  public KafkaConsumerConfig(String bootstrapServers, String groupId, Map<String, Object> extras) {
    this(bootstrapServers, groupId, extras, 0);
  }

  /**
   * Full constructor. {@code maxInFlightMessages} bounds the number of polled-but-unacked
   * messages: when at-or-above the cap, {@link KafkaConsumer#poll(java.time.Duration)} returns an
   * empty batch without calling the underlying Kafka client, so per-message bookkeeping cannot
   * grow without bound during a prolonged downstream outage. {@code 0} disables the cap.
   */
  public KafkaConsumerConfig(
      String bootstrapServers,
      String groupId,
      Map<String, Object> extras,
      int maxInFlightMessages) {
    this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
    this.groupId = Objects.requireNonNull(groupId, "groupId");
    this.extras =
        extras == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(extras));
    if (maxInFlightMessages < 0) {
      throw new IllegalArgumentException(
          "maxInFlightMessages must be >= 0, got " + maxInFlightMessages);
    }
    this.maxInFlightMessages = maxInFlightMessages;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public String getGroupId() {
    return groupId;
  }

  public Map<String, Object> getExtras() {
    return extras;
  }

  /**
   * In-flight cap above which {@link KafkaConsumer#poll(java.time.Duration)} short-circuits to an
   * empty batch. {@code 0} means uncapped.
   */
  public int getMaxInFlightMessages() {
    return maxInFlightMessages;
  }

  /** Converts this config to a Kafka client {@link Properties} bag. */
  public Properties toProperties() {
    Properties props = new Properties();
    for (Map.Entry<String, Object> e : extras.entrySet()) {
      props.put(e.getKey(), e.getValue());
    }
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    return props;
  }
}
