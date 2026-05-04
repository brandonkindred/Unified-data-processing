package com.unifieddataprocessing.pubsub.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

public final class KafkaConsumerConfig {

    private final String bootstrapServers;
    private final String groupId;
    private final Map<String, Object> extras;

    public KafkaConsumerConfig(String bootstrapServers, String groupId) {
        this(bootstrapServers, groupId, Collections.emptyMap());
    }

    public KafkaConsumerConfig(String bootstrapServers, String groupId, Map<String, Object> extras) {
        this.bootstrapServers = Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        this.extras = extras == null
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
