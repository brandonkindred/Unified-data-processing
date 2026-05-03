package com.unifieddataprocessing.pubsub;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class Message {

    private final String id;
    private final String topic;
    private final byte[] payload;
    private final Map<String, String> attributes;

    public Message(String id, String topic, byte[] payload, Map<String, String> attributes) {
        this.id = Objects.requireNonNull(id, "id");
        this.topic = Objects.requireNonNull(topic, "topic");
        this.payload = Objects.requireNonNull(payload, "payload");
        this.attributes = attributes == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(Map.copyOf(attributes));
    }

    public String getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    public byte[] getPayload() {
        return payload;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }
}
