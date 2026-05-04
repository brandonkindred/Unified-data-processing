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

  public KafkaConsumerConfig(String bootstrapServers, String groupId) {
    this(bootstrapServers, groupId, Collections.emptyMap());
  }

  /** Creates a config with extra Kafka client properties merged in (extras may be null). */
  public KafkaConsumerConfig(String bootstrapServers, String groupId, Map<String, Object> extras) {
    this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
    this.groupId = Objects.requireNonNull(groupId, "groupId");
    this.extras =
        extras == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(extras));
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
