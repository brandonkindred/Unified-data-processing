package com.unifieddataprocessing.pubsub.bridge;

import com.unifieddataprocessing.pubsub.Message;
import com.unifieddataprocessing.pubsub.schema.Schema;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Rewrites a source {@link Message} into the form republished onto the bridge's Kafka backbone:
 * the topic becomes the registration's target topic and the bridge-owned provenance attributes are
 * stamped onto the message, overwriting any caller-supplied values. When a {@link Schema} is
 * supplied — i.e. the bridge validated the payload against a registered schema — the schema
 * subject and version are stamped as well.
 */
final class MessageRewriter {

  private MessageRewriter() {}

  static Message rewrite(Message src, Registration reg) {
    return rewrite(src, reg, null);
  }

  static Message rewrite(Message src, Registration reg, Schema schema) {
    Map<String, String> attrs = new LinkedHashMap<>(src.getAttributes());
    attrs.put(BridgeAttributes.BRIDGE_SOURCE_ID, reg.sourceId());
    attrs.put(BridgeAttributes.BRIDGE_SOURCE_TOPIC, reg.sourceTopic());
    attrs.put(BridgeAttributes.BRIDGE_CHANNEL, reg.channel());
    if (schema != null) {
      attrs.put(BridgeAttributes.BRIDGE_SCHEMA_SUBJECT, schema.subject());
      attrs.put(BridgeAttributes.BRIDGE_SCHEMA_VERSION, Integer.toString(schema.version()));
    }
    return new Message(src.getId(), reg.targetTopic(), src.getPayload(), attrs);
  }
}
