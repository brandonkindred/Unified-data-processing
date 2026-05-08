package com.unifieddataprocessing.pubsub.kafka;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;

/** Configuration for the Kafka-backed {@link com.unifieddataprocessing.pubsub.PubSubPublisher}. */
public final class KafkaProducerConfig {

  private final String bootstrapServers;
  private final Map<String, Object> extras;

  public KafkaProducerConfig(String bootstrapServers) {
    this(bootstrapServers, Collections.emptyMap());
  }

  /** Creates a config with extra Kafka client properties merged in (extras may be null). */
  public KafkaProducerConfig(String bootstrapServers, Map<String, Object> extras) {
    this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
    this.extras =
        extras == null
            ? Collections.emptyMap()
            : Collections.unmodifiableMap(new LinkedHashMap<>(extras));
  }

  public String getBootstrapServers() {
    return bootstrapServers;
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
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    return props;
  }
}
