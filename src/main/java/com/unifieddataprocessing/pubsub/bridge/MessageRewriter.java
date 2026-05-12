package com.unifieddataprocessing.pubsub.bridge;

import com.unifieddataprocessing.pubsub.Message;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Rewrites a source {@link Message} into the form republished onto the bridge's Kafka backbone:
 * the topic becomes the registration's target topic and the bridge-owned provenance attributes are
 * stamped onto the message, overwriting any caller-supplied values.
 */
final class MessageRewriter {

  private MessageRewriter() {}

  static Message rewrite(Message src, Registration reg) {
    Map<String, String> attrs = new LinkedHashMap<>(src.getAttributes());
    attrs.put(BridgeAttributes.BRIDGE_SOURCE_ID, reg.sourceId());
    attrs.put(BridgeAttributes.BRIDGE_SOURCE_TOPIC, reg.sourceTopic());
    attrs.put(BridgeAttributes.BRIDGE_CHANNEL, reg.channel());
    return new Message(src.getId(), reg.targetTopic(), src.getPayload(), attrs);
  }
}
