package com.unifieddataprocessing.pubsub;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/** Immutable pub/sub message envelope carried across consumer implementations. */
public final class Message {

  private final String id;
  private final String topic;
  private final byte[] payload;
  private final Map<String, String> attributes;

  /** Creates a message; {@code attributes} may be {@code null} (treated as empty). */
  public Message(String id, String topic, byte[] payload, Map<String, String> attributes) {
    this.id = Objects.requireNonNull(id, "id");
    this.topic = Objects.requireNonNull(topic, "topic");
    this.payload = Objects.requireNonNull(payload, "payload");
    this.attributes =
        attributes == null
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
    return payload.clone();
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }
}
